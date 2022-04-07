const { knex } = require('../util/db')
const { APIError, useAPIErrorOptions } = require('../util/api-error')
const { lambda } = require('../util/aws')
const { getContext, ERROR_QL_CTX } = require('../util/context')
const { getView, getQueryViews } = require('./views')
const { insertGeoIntersectsInTree } = require('./geo-intersects')
const { parseQueryToTree, ParserError } = require('./parser')
const { executeQueryInStreamMode } = require('./engine')
// const { executeQuery } = require('./engine')
const { putToS3Cache, getFromS3Cache, getS3CacheURL, queryWithCache } = require('../util/cache')
const { typeToCatMap, CAT_STRING } = require('./type')
const { isInternalUser, sortViewDependencies } = require('./utils')
const {
  QL_SCHEMA,
  EXECUTION_BUCKET,
  LAMBDA_EXECUTOR_ARN,
  STATUS_QUEUED,
  STATUS_SOURCING,
  STATUS_RUNNING,
  STATUS_RETRYING,
  STATUS_SUCCEEDED,
  STATUS_CANCELLED,
  STATUS_FAILED,
  // RESULTS_PART_SIZE,
  // RESULTS_PART_SIZE_FIRST,
  MAX_LENGTH_EXECUTION_TOKEN,
  MAX_LENGTH_STATUS_REASON,
} = require('./constants')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

/**
 * Returns an array of execution metas based on the supplied filters
 * @param {Object} [filters]
 * @param {number} [filters.executionID] Execution ID
 * @param {-1|number[]} [filters.whitelabelIDs] Array of whitelabel IDs (agency ID)
 * @param {-1|number[]} [filters.customerIDs] Array of customer IDs (agency ID)
 * @param {number} [filters.queryID] Saved query ID
 * @param {string} [filters.queryHash] Query hash (unique to the query)
 * @param {string} [filters.columnHash] Column hash (unique to the name/type of the results)
 * @param {string} [filters.status] Status of interest
 * @param {number} [filters.start] Unix timestamp (in seconds) from which to consider records
 * @param {number} [filters.end] Unix timestamp (in seconds) up to which to consider records
 * @param {string} [filters.clientToken] Client supplied execution ID (unique at WL/CU level)
 * @param {boolean} [filters.hideInternal=false] Whether or not to filter out queries
 * using internal fields
 * @param {number} [filters.limit] The max number of executions to return (sorted by date DESC)
 * @returns {Promise<Array>}
 */
const getExecutionMetas = async ({
  executionID,
  whitelabelIDs,
  customerIDs,
  queryID,
  queryHash,
  columnHash,
  status,
  start,
  end,
  clientToken,
  hideInternal = false,
  limit,
} = {}) => {
  // join with queries so as to hide the queryID when the query has been deleted (i.e. inactive)
  const { rows } = await knex.raw(`
    SELECT
      e.execution_id AS "executionID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      c.timezone AS "customerTimezone",
      e.query_hash AS "queryHash",
      e.column_hash AS "columnHash",
      e.status,
      e.status_ts AS "statusTS",
      q.query_id AS "queryID",
      e.query,
      e.view_ids AS "viewIDs",
      e.columns,
      e.is_internal AS "isInternal",
      e.query_id IS NOT NULL AND q.query_id IS NULL AS "isOrphaned",
      e.cost,
      CASE WHEN e.results_parts IS NOT NULL THEN
        ARRAY(
          SELECT
            jsonb_build_object(
              'part', row_number() OVER (),
              'firstIndex', lag(rp + 1, 1, 0) over(),
              'lastIndex', rp
            )
          FROM UNNEST(e.results_parts) rp
        )
      END AS "resultsParts",
      CASE WHEN e.results_parts IS NOT NULL THEN
        COALESCE(e.results_parts[array_length(e.results_parts, 1)] + 1, 0)
      END AS "resultsSize",
      s.cron AS "scheduleCron",
      sj.job_ts AS "scheduleTS"
    FROM ${QL_SCHEMA}.executions e
    JOIN public.customers c ON c.customerid = e.customer_id
    LEFT JOIN ${QL_SCHEMA}.queries q ON q.query_id = e.query_id AND q.is_active
    LEFT JOIN ${QL_SCHEMA}.schedule_jobs sj ON sj.job_id = e.schedule_job_id
    LEFT JOIN ${QL_SCHEMA}.schedules s ON s.schedule_id = sj.schedule_id
    WHERE
      TRUE
      ${executionID ? 'AND e.execution_id = :executionID' : ''}
      ${whitelabelIDs && whitelabelIDs !== -1 ? 'AND c.whitelabelid = ANY(:whitelabelIDs)' : ''}
      ${customerIDs && customerIDs !== -1 ? 'AND e.customer_id = ANY(:customerIDs)' : ''}
      ${queryID ? 'AND q.query_id = :queryID' : ''}
      ${queryHash ? 'AND e.query_hash = :queryHash' : ''}
      ${columnHash ? 'AND e.column_hash = :columnHash' : ''}
      ${status ? 'AND e.status = :status' : ''}
      ${start ? 'AND e.status_ts >= to_timestamp(:start)' : ''}
      ${end ? 'AND e.status_ts <= to_timestamp(:end)' : ''}
      ${clientToken ? 'AND e.client_token = :clientToken' : ''}
      ${hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ORDER BY 1 DESC
    ${limit ? 'LIMIT :limit' : ''}
  `, {
    executionID,
    whitelabelIDs,
    customerIDs,
    queryID,
    queryHash,
    columnHash,
    status,
    start,
    end,
    clientToken,
    limit,
  })
  return rows
}

/**
 * Deterministically generates a cache key for the execution results
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {number} [part] Results part number
 * @returns {string} Cache key
 */
const getExecutionResultsKey = (customerID, executionID, part) =>
  `${customerID}/${executionID}${part ? `/${part}` : ''}`

/**
 * Pulls the execution results from storage
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {Object} options
 * @param {boolean} [options.parseFromJson=true] Whether or not to parse the results into an object
 * @param {number} [options.maxSize] Max size of the results in bytes
 * @param {number} [options.part] Results part number (required for multi-part results)
 * @returns {Promise<string|Object[]|undefined>} Query results or undefined if not found
 * or too large
 */
const getExecutionResults = (
  customerID,
  executionID,
  { parseFromJson = true, maxSize, part } = {},
) => getFromS3Cache(
  getExecutionResultsKey(customerID, executionID, part),
  { bucket: EXECUTION_BUCKET, parseFromJson, maxSize },
)

/**
 * Pulls multi-part execution results from storage given an array of part numbers
 * Parts are concatenated in accordance with their part number in ascending order
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {number[]} parts Results parts to pull from cache
 * @param {boolean} [parseFromJson=true] Whether or not to parse the results into an object
 * @returns {Promise<string|Object[]|undefined>} Query results or undefined if not found
 */
const getExecutionResultsParts = async (customerID, executionID, parts, parseFromJson = true) => {
  const rawParts = (await Promise
    // get all parts from s3
    .all(parts
      .sort((a, b) => a - b)
      .map(p => getFromS3Cache(
        getExecutionResultsKey(customerID, executionID, p),
        { bucket: EXECUTION_BUCKET, parseFromJson },
      ))))
    // filter out undefined (part does not exist)
    .filter(p => p)
    .reduce((acc, p, i, allParts) => {
      // if json, push individual objects in acc
      if (parseFromJson) {
        acc.push(...p)
        return acc
      }
      // else push entire parts as strings
      // remove square brackets between parts
      acc.push(p.slice(
        !i ? 0 : 1, // keep entry bracket on first part
        i === allParts.length - 1 ? p.length : -1, // keep exit bracket on last part
      ))
      return acc
    }, [])
  // no results
  if (!rawParts.length) {
    return
  }
  return parseFromJson ? rawParts : rawParts.join(', ')
}

/**
 * Pulls the execution results from storage
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {Object} options
 * @param {{part: number, firstIndex: number, lastIndex: number}[]} [options.resultsParts] List of
 * the execution's results parts. Required for queries with multi-part results
 * @param {boolean} [options.parseFromJson=true] Whether or not to parse the results into an object
 * @returns {Promise<string|Object[]|undefined>} Query results or undefined if not found
 * or too large
 */
const getAllExecutionResults = (
  customerID,
  executionID,
  { resultsParts, parseFromJson = true },
) => {
  // multi-part
  if (resultsParts) {
    // too large
    if (resultsParts.length > 3) {
      return
    }
    return resultsParts.length
      ? getExecutionResultsParts(
        customerID,
        executionID,
        resultsParts.map(({ part }) => part),
        parseFromJson,
      )
      : []
  }
  // legacy single part
  return getExecutionResults(customerID, executionID, { parseFromJson })
}

/**
 * Returns a temporary URL to the execution results
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {Object} options
 * @param {number} [options.ttl=900] URL validity in seconds. Defaults to 900 (15 minutes)
 * @param {number} [options.part] Results part number
 * @returns {Promise<string|undefined>} URL to the query results or undefined if not found
 */
const getExecutionResultsURL = (customerID, executionID, { ttl = 900, part } = {}) =>
  queryWithCache( // cache pre-signed url for ttl seconds
    ['execution-results', executionID, part],
    () => getS3CacheURL(
      getExecutionResultsKey(customerID, executionID, part),
      { bucket: EXECUTION_BUCKET, ttl },
    ),
    { ttl, maxAge: ttl, gzip: false, json: false },
  )

/**
 * Creates an execution
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} queryHash Query hash (unique to the query)
 * @param {string} columnHash Column hash (unique to the name/type of the results)
 * @param {Object} query Query object
 * @param {string[]} viewsIDs List of the query views' IDs
 * @param {[string, number][]} columns List of the query columns formatted as [name, pgTypeOID]
 * @param {boolean} isInternal Whether or not the query accesses views restricted to internal users
 * @param {Object.<string, number[]>} dependencies Dynamic views the query depends on (e.g. log or
 * ext views). In the form {dep_type: dep_id[]}
 * @param {Object} [options] Optional args
 * @param {number} [options.queryID] If the execution is tied to a saved query, the id of such query
 * @param {string} [options.status] Initial status
 * @param {number} [options.scheduleJobID] The ID of the schedule job which triggered the
 * execution, if any
 * @param {string} [options.clientToken] A client supplied token unique at the WL/CU level
 * @param {Knex} [options.knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @returns {Promise<{executionID:number, isCreated:boolean}>} The execution ID along with a boolean
 * indicating whether or not the returned id is that of an existing execution (in the event
 * of a duplicate idempotent submission)
 */
const createExecution = async (
  whitelabelID,
  customerID,
  queryHash,
  columnHash,
  query,
  viewIDs,
  columns,
  isInternal,
  dependencies,
  { queryID, status, scheduleJobID, clientToken, knexClient = knex } = {},
) => {
  const cols = [
    ['customer_id', ':customerID'],
    ['query_hash', ':queryHash'],
    ['column_hash', ':columnHash'],
    ['query', ':query'],
    ['view_ids', ':viewIDs'],
    ['columns', ':columns'],
    ['is_internal', ':isInternal'],
  ]
  const values = {
    whitelabelID,
    customerID,
    queryHash,
    columnHash,
    query: JSON.stringify(query),
    viewIDs: JSON.stringify(viewIDs),
    columns: JSON.stringify(columns),
    isInternal,
  }

  if (dependencies && Object.keys(dependencies).length) {
    cols.push(['dependencies', ':dependencies'])
    values.dependencies = JSON.stringify(dependencies)
  }

  if (queryID) {
    cols.push(['query_id', ':queryID'])
    values.queryID = queryID
  }

  if (status) {
    cols.push(['status', ':status'])
    values.status = status
  }

  if (scheduleJobID) {
    cols.push(['schedule_job_id', ':scheduleJobID'])
    values.scheduleJobID = scheduleJobID
  }

  if (clientToken) {
    cols.push(['client_token', ':clientToken'])
    values.clientToken = clientToken.slice(0, MAX_LENGTH_EXECUTION_TOKEN)
  }

  // unique constraints:
  // - customerID, clientToken
  // - queryID, scheduleJobID
  const existingFilters = []
  if (clientToken) {
    existingFilters.push('client_token = :clientToken')
  }
  if (queryID && scheduleJobID) {
    existingFilters.push('(query_id = :queryID AND schedule_job_id = :scheduleJobID)')
  }
  const existing = existingFilters.length ? `
    existing AS (
      SELECT
        execution_id AS "executionID",
        query_hash = :queryHash AS "isIdempotent",
        FALSE AS "isCreated"
      FROM ${QL_SCHEMA}.executions
      WHERE
        EXISTS (SELECT * FROM access)
        AND customer_id = :customerID
        AND (${existingFilters.join(' OR ')})
    ),
  ` : ''

  const { rows: [{ executionID, isIdempotent, isCreated } = {}] } = await knexClient.raw(`
    WITH access AS (
      SELECT 1 FROM public.customers
      WHERE
        whitelabelid = :whitelabelID
        AND customerid = :customerID
    ),
    ${existing}
    new AS (
      INSERT INTO ${QL_SCHEMA}.executions
        (${cols.map(([col]) => col).join(', ')})
        SELECT ${cols.map(([, val]) => val).join(', ')}
        WHERE
          EXISTS (SELECT * FROM access)
          ${existing ? 'AND NOT EXISTS (SELECT * FROM existing)' : ''}
      ON CONFLICT DO NOTHING
      RETURNING
        execution_id AS "executionID",
        TRUE AS "isIdempotent",
        TRUE AS "isCreated"
    )
    ${existing ? 'SELECT * FROM existing UNION' : ''}
    SELECT * FROM new
  `, values)

  if (!isIdempotent) {
    throw apiError('Execution is non-idempotent', 400)
  }

  return { executionID, isCreated }
}

/**
 * Updates an execution based on its id
 * @param {number} executionID Execution ID
 * @param {Object} updates
 * @param {string} [updates.status] New status
 * @param {string} [updates.statusReason] Reason for status update. Will be disregarded when
 * no value is supplied for status
 * @param {Object} [updates.queryID] New query ID to attach the execution to
 * @param {number[]} [updates.resultsParts] Array of part end indexes (relative
 * to the full result set - e.g. [10000, 20000, 30000])
 * @param {Object} options
 * @param {string[]} [options.optOutStatuses] Execution statuses for which to skip the update
 * @param {Knex} [options.knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 */
const updateExecution = async (
  executionID,
  { status, statusReason, queryID, resultsParts },
  { optOutStatuses, knexClient = knex } = {},
) => {
  const columns = []
  const values = []
  const expressions = []
  if (status) {
    columns.push('status', 'status_reason')
    values.push(status, statusReason ? statusReason.slice(0, MAX_LENGTH_STATUS_REASON) : null)
    expressions.push('status_ts = now()')
  }
  if (queryID) {
    columns.push('query_id')
    values.push(queryID)
  }
  if (resultsParts) {
    columns.push('results_parts')
    values.push(resultsParts)
  }
  if (!columns.length && !expressions.length) {
    // nothing to update
    return
  }
  // values.push(executionID)
  await knexClient.raw(`
    UPDATE ${QL_SCHEMA}.executions
    SET ${columns.map(col => `${col} = ?`).concat(expressions).join(', ')}
    WHERE
      execution_id = ?
      ${optOutStatuses ? `AND NOT (status = ANY(?::${QL_SCHEMA}.ml_status[]))` : ''}
  `, [...values, executionID, optOutStatuses])
}

/**
 * Triggers the execution's execution step
 * @param {number} executionID Execution ID
 */
const triggerExecution = async (executionID) => {
  try {
    if (!LAMBDA_EXECUTOR_ARN) {
      throw new Error('Lambda executor env variable not set')
    }
    const res = await lambda.invoke({
      FunctionName: LAMBDA_EXECUTOR_ARN,
      InvocationType: 'Event',
      Payload: JSON.stringify({ execution_id: executionID }),
    }).promise()
    if (res.StatusCode !== 202) {
      throw new Error(`Lambda responded with status code: ${res.StatusCode}`)
    }
  } catch (err) {
    // don't bubble up the error; Airflow will take over from here
    console.log('Failed to invoke ML executor', err.message)
    const statusReason = [err.message, err.originalError].reduce((acc, msg) => {
      if (msg) {
        return `${acc} - ${msg}`
      }
      return acc
    }, 'Executor invocation error')
    await updateExecution(
      executionID,
      { status: STATUS_RETRYING, statusReason },
      { optOutStatuses: [STATUS_CANCELLED] },
    )
  }
}

/**
 * Creates an entry for the execution in the database and triggers such execution when
 * it is dependency-free
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} queryHash Query hash (unique to the query)
 * @param {string} columnHash Column hash (unique to the name/type of the results)
 * @param {Object} query Query object
 * @param {Object} views Views
 * @param {[string, number][]} columns List of the query columns formatted as [name, pgTypeOID]
 * @param {Object} [options] Optional args
 * @param {number} [options.queryID] If the execution is tied to a saved query, the ID of such query
 * @param {number} [options.scheduleJobID] The ID of the schedule job which triggered the
 * execution, if any
 * @param {string} [options.clientToken] A client supplied token unique at the WL/CU level
 * @returns {Promise<number>} The execution ID
 */
const queueExecution = async (
  whitelabelID,
  customerID,
  queryHash,
  columnHash,
  query,
  views,
  columns,
  { queryID, scheduleJobID, clientToken } = {},
) => {
  const viewIDs = Object.keys(views)
  const dependencies = sortViewDependencies(views)
  // if no dependencies, can start execution right away
  const status = Object.keys(dependencies).length ? STATUS_QUEUED : STATUS_RUNNING
  // determine whether or not query uses internal-only views
  const isInternal = Object.values(views).some(v => v.isInternal)
  // insert into executions
  const { executionID, isCreated } = await createExecution(
    whitelabelID,
    customerID,
    queryHash,
    columnHash,
    query,
    viewIDs,
    columns,
    isInternal,
    dependencies,
    { queryID, status, scheduleJobID, clientToken },
  )
  // trigger execution when no deps
  if (isCreated && status === STATUS_RUNNING) {
    await triggerExecution(executionID)
  }
  return executionID
}

// extracts async and saved queries and queues them as executions
const queueExecutionMW = async (req, res, next) => {
  try {
    const { queryID } = req.ql.query || req.ql.execution || {}
    const {
      access,
      body: { clientToken },
      ql: { views, tree },
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
    if (
      clientToken !== undefined
      && (
        typeof clientToken !== 'string'
        || !clientToken
        || clientToken.length > MAX_LENGTH_EXECUTION_TOKEN
      )
    ) {
      throw apiError('Invalid client token', 400)
    }
    const query = tree.toQL({ keepParamRefs: false })
    const executionID = await queueExecution(
      access.whitelabel[0],
      access.customers[0],
      mlQueryHash,
      mlQueryColumnHash,
      query,
      views,
      mlQueryColumns,
      { queryID, clientToken },
    )
    res.json({ executionID })
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to queue the query execution', 500))
  }
}

const previewExecutionMW = async (req, res, next) => {
  try {
    const { preview } = req.query
    if (!['1', 'true'].includes((preview || '').toLowerCase())) {
      return next()
    }
    const { queryID } = req.ql.query || req.ql.execution || {}
    const {
      access,
      ql: { views: queryViews, tree },
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
    const query = tree.toQL({ keepParamRefs: false })
    const sql = tree.toSQL({ keepParamRefs: false })
    const { whitelabel, customers } = access

    // convert columns from array to object
    const columns = mlQueryColumns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))

    // populate views
    const views = []
    await Promise.all(Object.keys(queryViews).map(id =>
      getView(access, id).then(({ name, view }) => {
        view.name = name
        views.push(view)
      })))

    // respond with execution meta
    res.json({
      whitelabelID: whitelabel[0],
      customerID: customers[0],
      queryHash: mlQueryHash,
      columnHash: mlQueryColumnHash,
      queryID,
      query,
      sql,
      views,
      columns,
      isInternal: Object.values(queryViews).some(v => v.isInternal),
      // cost: 1,
    })
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to evaluate the execution', 500))
  }
}

// const writeExecutionResults = async (
//   customerID, executionID, results,
//   { partSize = RESULTS_PART_SIZE, firtPartSize = RESULTS_PART_SIZE_FIRST } = {},
// ) => {
//   // split results into parts
//   const resultsParts = []
//   const cacheParts = []
//   let partStart = 0
//   while (partStart < results.length) {
//     const size = resultsParts.length ? partSize : firtPartSize
//     const partEnd = Math.min(partStart + size, results.length) - 1
//     // part will be referred to by its end index relative to the result set
//     resultsParts.push(partEnd)
//     // persist part to S3
//     cacheParts.push(putToS3Cache(
//       getExecutionResultsKey(customerID, executionID, resultsParts.length),
//       results.slice(partStart, partEnd + 1),
//       { gzip: true, json: true, bucket: EXECUTION_BUCKET },
//     ))
//     partStart = partEnd + 1
//   }
//   await Promise.all(cacheParts)
//   return resultsParts
// }

// let errors bubble up so the query can be retried
const runExecution = async (executionID, engine = 'pg') => {
  try {
    const [execution] = await getExecutionMetas({ executionID })
    if (!execution) {
      throw apiError('Invalid execution ID')
    }
    const { whitelabelID, customerID, query, columns, isInternal, status } = execution
    if (status !== STATUS_RUNNING) {
      // don't run unless the status was set to running beforehand
      return
    }
    const access = {
      whitelabel: [whitelabelID],
      customers: [customerID],
      prefix: isInternal ? 'internal' : 'customers',
    }
    // parse to query tree
    let tree = parseQueryToTree(query, { type: 'ql' })
    // get view queries
    const views = await getQueryViews(access, tree.viewColumns, engine)
    // to support legacy geo joins (i.e. strict equality b/w two geo columns)
    tree = insertGeoIntersectsInTree(views, tree)
    // // run query
    // eslint-disable-next-line max-len
    // const res = await executeQuery(whitelabelID, customerID, views, tree, { engine, executionID })
    // // split results into parts
    // const resultsParts = await writeExecutionResults(customerID, executionID, res)

    const partLengths = {}
    await executeQueryInStreamMode(
      whitelabelID,
      customerID,
      views,
      tree,
      columns,
      (rows, i) => {
        partLengths[i] = rows.length
        return putToS3Cache(
          getExecutionResultsKey(customerID, executionID, i + 1),
          rows,
          { gzip: true, json: true, bucket: EXECUTION_BUCKET },
        )
      },
      { engine, executionID },
    )
    const resultsParts = Object.entries(partLengths)
      .sort(([a], [b]) => a - b)
      .reduce((acc, [, partLength]) => {
        // end index relative to the entire result set
        const previousPartEnd = acc.slice(-1)[0] || -1
        acc.push(previousPartEnd + partLength)
        return acc
      }, [])

    // update status to succeeded + breakdown of parts
    await updateExecution(
      executionID,
      { status: STATUS_SUCCEEDED, resultsParts },
      { optOutStatuses: [STATUS_CANCELLED] },
    )
  } catch (err) {
    // let the listeners know that the function might be retried
    const statusReason = [err.message, err.originalError].reduce((acc, msg) => {
      if (msg) {
        return `${acc} - ${msg}`
      }
      return acc
    }, 'Executor error')
    await updateExecution(
      executionID,
      { status: STATUS_RETRYING, statusReason },
      { optOutStatuses: [STATUS_CANCELLED] },
    )
    throw err
  }
}

// lambda handler
const executionHandler = ({ execution_id, engine = 'pg' }) => {
  // eslint-disable-next-line radix
  const id = parseInt(execution_id, 10)
  if (Number.isNaN(id)) {
    throw apiError(`Invalid execution ID: ${execution_id}`)
  }
  if (engine !== 'pg' && engine !== 'trino') {
    throw apiError(`Invalid engine: ${engine}`)
  }
  console.log('execution id', id, 'engine', engine)
  return runExecution(id, engine)
}

// isRequired flags whether or not 'execution' is a mandatory route/query param
const loadExecution = (isRequired = true) => async (req, _, next) => {
  try {
    if (req.ql.query) {
      // illegal to populate both req.ql.query and req.ql.execution
      return next()
    }
    const id = req.params.id || req.query.execution
    // eslint-disable-next-line radix
    const executionID = parseInt(id, 10)
    if (Number.isNaN(executionID)) {
      if (isRequired) {
        throw apiError('Invalid execution ID')
      }
      return next()
    }
    const { access } = req
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = access
    const hideInternal = !isInternalUser(prefix)
    const [execution] = await getExecutionMetas({
      executionID,
      whitelabelIDs,
      customerIDs,
      hideInternal,
    })
    if (!execution) {
      throw apiError('Invalid execution ID', 404)
    }
    // attach to req
    req.ql.execution = execution
    getContext(req, ERROR_QL_CTX).executionID = executionID
    // set customer to that of the execution
    req.access = {
      ...access,
      whitelabel: [execution.whitelabelID],
      customers: [execution.customerID],
    }
    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to load the execution', 500))
  }
}

const respondWithExecution = async (req, res, next) => {
  try {
    const { execution } = req.ql
    const { executionID, customerID, status, viewIDs, query, columns, resultsParts } = execution
    const { results } = req.query
    // attach results
    // TODO: deprecate - retrieve results via results route
    if (['1', 'true'].includes((results || '').toLowerCase()) && status === STATUS_SUCCEEDED) {
      // multi-part
      execution.results = await getAllExecutionResults(
        customerID,
        executionID,
        { resultsParts },
      )
    }
    // convert columns from array to object
    execution.columns = columns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))
    // populate views
    delete execution.viewIDs
    execution.views = []
    const viewColumns = {}
    let hasAllViews = true
    await Promise.all(viewIDs.map(id => getView(req.access, id).then(({ name, view, columns }) => {
      view.name = name
      execution.views.push(view)
      viewColumns[id] = { columns }
    }).catch((err) => {
      // edge case when view has been unsubscribed or is no longer available
      // soft fail
      execution.views.push({
        id,
        error: (err instanceof APIError && err.message) || 'View could not be retrieved',
      })
      hasAllViews = false
    })))
    // parse to query tree
    let tree = parseQueryToTree(query, { type: 'ql' })
    // replace legacy geo joins with geo_intersects
    if (hasAllViews) {
      tree = insertGeoIntersectsInTree(viewColumns, tree)
    }
    // rewrite query
    execution.query = tree.toQL({ keepParamRefs: false })
    // attach sql
    execution.sql = tree.toSQL({ keepParamRefs: false })

    res.json(execution)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the execution', 500))
  }
}

const respondWithOrRedirectToExecutionResultsURL = async (req, res, next) => {
  try {
    const { executionID, customerID, status, resultsParts } = req.ql.execution
    const { redirect } = req.query
    const { part } = req.params
    if (status !== STATUS_SUCCEEDED) {
      throw apiError(`The execution is not in '${STATUS_SUCCEEDED}' status`, 400)
    }
    let safePart
    if (resultsParts) {
      if (!resultsParts.length) {
        // empty result set
        throw apiError('This execution returned no results')
      }
      if (!part) {
        throw apiError('Please provide a part number')
      }
      safePart = parseInt(part)
      if (Number.isNaN(safePart) || safePart <= 0 || safePart > resultsParts.length) {
        throw apiError(`Invalid results part number: ${part}`)
      }
    }
    // generate/retrieve URL to results in storage
    const url = await getExecutionResultsURL(customerID, executionID, { part: safePart })
    if (['1', 'true'].includes((redirect || '').toLowerCase())) {
      return res.redirect(302, url)
    }
    res.json({ url })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the execution results', 500))
  }
}

const listExecutions = async (req, res, next) => {
  try {
    const { query, results, limit, qhash, chash, status, start, end, token } = req.query
    // sanitize query params
    let queryID
    if (query) {
      // eslint-disable-next-line radix
      queryID = parseInt(query, 10)
      if (Number.isNaN(queryID)) {
        throw apiError(`Invalid query ID: ${query}`)
      }
    }
    let safeLimit
    if (limit) {
      // eslint-disable-next-line radix
      safeLimit = parseInt(limit, 10)
      if (Number.isNaN(safeLimit) || limit < 1) {
        throw apiError(`Invalid limit: ${limit}`)
      }
    }
    let safeStatus
    if (status) {
      safeStatus = (status || '').toUpperCase()
      if (![
        STATUS_QUEUED,
        STATUS_SOURCING,
        STATUS_RUNNING,
        STATUS_RETRYING,
        STATUS_SUCCEEDED,
        STATUS_FAILED,
      ].includes(safeStatus)) {
        throw apiError(`Invalid status: ${status}`)
      }
    }
    let safeStart
    if (start) {
      // eslint-disable-next-line radix
      safeStart = parseInt(start, 10)
      if (Number.isNaN(safeStart) || safeStart < 0) {
        throw apiError(`Invalid start timestamp: ${start}`)
      }
    }
    let safeEnd
    if (end) {
      // eslint-disable-next-line radix
      safeEnd = parseInt(end, 10)
      if (Number.isNaN(safeEnd) || safeEnd < 0) {
        throw apiError(`Invalid end timestamp: ${end}`)
      }
    }
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
    const hideInternal = !isInternalUser(prefix)
    const executions = await getExecutionMetas({
      whitelabelIDs,
      customerIDs,
      hideInternal,
      queryID,
      queryHash: qhash,
      columnHash: chash,
      status: safeStatus,
      start: safeStart,
      end: safeEnd,
      clientToken: token,
      limit: safeLimit,
    })
    if (
      ['1', 'true'].includes((results || '').toLowerCase())
      && executions.length === 1
      && executions[0].status === STATUS_SUCCEEDED
    ) {
      executions[0].results = await getAllExecutionResults(
        executions[0].customerID,
        executions[0].executionID,
        { resultsParts: executions[0].resultsParts },
      )
    }
    // populate views
    // prepare response
    const viewMemo = {}
    await Promise.all(executions.reduce((acc, e) => {
      const { viewIDs, columns } = e

      // convert columns from array to object
      e.columns = columns.map(([name, pgType]) => ({
        name,
        category: typeToCatMap.get(pgType) || CAT_STRING,
      }))

      // populate views
      delete e.viewIDs
      e.views = e.views || []
      acc.push(...viewIDs.map((id) => {
        // memoize promises
        if (!(id in viewMemo)) {
          viewMemo[id] = getView(req.access, id)
            .then(({ name, view }) => {
              view.name = name
              return view
            })
            .catch(err => ({
              // edge case when view has been unsubscribed or is no longer available
              // soft fail
              id,
              error: (err instanceof APIError && err.message) || 'View could not be retrieved',
            }))
        }
        return viewMemo[id].then((view) => {
          e.views.push(view)
        })
      }))
      return acc
    }, []))
    res.json(executions)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the executions', 500))
  }
}

const cancelExecution = async (req, res, next) => {
  try {
    const { executionID, status } = req.ql.execution
    if ([STATUS_CANCELLED, STATUS_FAILED, STATUS_SUCCEEDED].includes(status)) {
      throw apiError('Execution is not in a cancellable status', 400)
    }
    await updateExecution(
      executionID,
      { status: STATUS_CANCELLED },
      { optOutStatuses: [STATUS_CANCELLED, STATUS_FAILED, STATUS_SUCCEEDED] },
    )
    res.json({ executionID })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to cancel the execution', 500))
  }
}

// to test out handler (replace ID)
// executionHandler({ execution_id: '748' }).then(() => console.log('ml execution done'))

module.exports = {
  createExecution,
  updateExecution,
  queueExecution,
  queueExecutionMW,
  previewExecutionMW,
  executionHandler,
  loadExecution,
  respondWithExecution,
  respondWithOrRedirectToExecutionResultsURL,
  listExecutions,
  cancelExecution,
  getExecutionMetas,
  getAllExecutionResults,
}

const { knex, mlPool, newPGClientFromPoolConfig } = require('../util/db')
const { APIError, useAPIErrorOptions } = require('../util/api-error')
const { lambda } = require('../util/aws')
const { getContext, ERROR_QL_CTX } = require('../util/context')
const { getView, getQueryViews } = require('./views')
const { insertGeo } = require('./geo')
const { executeQuery, establishFdwConnections } = require('./engine')
const { putToS3Cache, getFromS3Cache, getS3CacheURL, queryWithCache } = require('./cache')
const { typeToCatMap, CAT_STRING } = require('./type')
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
  RESULTS_PART_SIZE,
  RESULTS_PART_SIZE_FIRST,
} = require('./constants')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })
const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

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
 * @param {Knex} [options.knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @param {Date} [options.scheduleJobID] The ID of the schedule job which triggered the
 * execution, if any
 * @returns {Promise<number>} The execution ID or undefined when a SQL conflict is encountered
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
  { queryID, status, knexClient = knex, scheduleJobID } = {},
) => {
  const cols = [
    'customer_id',
    'query_hash',
    'column_hash',
    'query',
    'view_ids',
    'columns',
    'is_internal',
  ]
  const values = [
    whitelabelID,
    customerID,
    customerID,
    queryHash,
    columnHash,
    JSON.stringify(query),
    JSON.stringify(viewIDs),
    JSON.stringify(columns),
    isInternal,
  ]

  if (dependencies && Object.keys(dependencies).length) {
    cols.push('dependencies')
    values.push(JSON.stringify(dependencies))
  }

  if (queryID) {
    cols.push('query_id')
    values.push(queryID)
  }

  if (status) {
    cols.push('status')
    values.push(status)
  }

  if (scheduleJobID) {
    cols.push('schedule_job_id')
    values.push(scheduleJobID)
  }

  const { rows: [{ executionID } = {}] } = await knexClient.raw(`
    WITH access AS (
      SELECT customerid FROM public.customers
      WHERE
        whitelabelid = ?
        AND customerid = ?
    ),
    INSERT INTO ${QL_SCHEMA}.executions
      (${cols.join(', ')})
    VALUES
      (${cols.map(() => '?').join(', ')})
    WHERE EXISTS (SELECT * FROM access)
    ON CONFLICT DO NOTHING
    RETURNING execution_id AS "executionID"
  `, values)

  return executionID
}

/**
 * Updates an execution based on its id
 * @param {number} executionID Execution ID
 * @param {Object} updates
 * @param {string} [updates.status] New status
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
  { status, queryID, resultsParts },
  { optOutStatuses, knexClient = knex } = {},
) => {
  const columns = []
  const values = []
  const expressions = []
  if (status) {
    columns.push('status')
    values.push(status)
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
 * Sorts view dependencies by type and removes duplicates
 * @param {Object.<string, [string, number][]>} viewDependencies Dependencies per views
 * @returns {Object.<string, number[]>} Unique dependencies per type
 */
const sortViewDependencies = (viewDependencies) => {
  const deps = Object.values(viewDependencies).reduce((uniqueDeps, viewDeps) => {
    if (viewDeps) {
      viewDeps.forEach(([type, id]) => {
        uniqueDeps[type] = uniqueDeps[type] || new Set()
        uniqueDeps[type].add(id)
      })
    }
    return uniqueDeps
  }, {})
  Object.entries(deps).forEach(([key, value]) => {
    deps[key] = [...value]
  })
  return deps
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
    await updateExecution(
      executionID,
      { status: STATUS_RETRYING },
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
 * @param {Object.<string, Knex.QueryBuilder|Knex.Raw>} views Map of view ID's and knex
 * view objects
 * @param {Object.<string, [string, number][]>} viewDependencies Map of view ID's and
 * dependency arrays
 * @param {Object.<string, boolean>} viewIsInternal Map of view ID's and internal flags
 * @param {[string, number][]} columns List of the query columns formatted as [name, pgTypeOID]
 * @param {Object} [options] Optional args
 * @param {number} [options.queryID] If the execution is tied to a saved query, the ID of such query
 * @param {Date} [options.scheduleJobID] The ID of the schedule job which triggered the
 * execution, if any
 * @returns {Promise<number>} The execution ID or undefined
 */
const queueExecution = async (
  whitelabelID,
  customerID,
  queryHash,
  columnHash,
  query,
  views,
  viewDependencies,
  viewIsInternal,
  columns,
  { queryID, scheduleJobID } = {},
) => {
  const viewIDs = Object.keys(views)
  const dependencies = sortViewDependencies(viewDependencies)
  // if no dependencies, can start execution right away
  const status = Object.keys(dependencies).length ? STATUS_QUEUED : STATUS_RUNNING
  // determine whether or not query uses internal-only views
  const isInternal = Object.values(viewIsInternal).some(is => is)
  // insert into executions
  const executionID = await createExecution(
    whitelabelID,
    customerID,
    queryHash,
    columnHash,
    query,
    viewIDs,
    columns,
    isInternal,
    dependencies,
    { queryID, status, scheduleJobID },
  )
  // trigger execution when no deps
  if (executionID && status === STATUS_RUNNING) {
    await triggerExecution(executionID)
  }
  return executionID
}

// extracts async and saved queries and queues them as executions
const queueExecutionMW = async (req, res, next) => {
  try {
    const { queryID, query: loadedQuery } = req.mlQuery || req.mlExecution || {}
    const { query } = req.body
    const {
      access,
      mlViews,
      mlViewDependencies,
      mlViewIsInternal,
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
    const executionID = await queueExecution(
      access.whitelabel[0],
      access.customers[0],
      mlQueryHash,
      mlQueryColumnHash,
      loadedQuery || query,
      mlViews,
      mlViewDependencies,
      mlViewIsInternal,
      mlQueryColumns,
      { queryID },
    )
    if (!executionID) {
      throw apiError('Execution already exists', 400)
    }
    res.json({ executionID })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to queue the query execution', 500))
  }
}

const previewExecutionMW = async (req, res, next) => {
  try {
    const { preview } = req.query
    if (!['1', 'true'].includes((preview || '').toLowerCase())) {
      return next()
    }
    const { queryID, query: loadedQuery } = req.mlQuery || req.mlExecution || {}
    const { query } = req.body
    const {
      access,
      mlViews,
      mlViewIsInternal,
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
    const { whitelabel, customers } = access

    // convert columns from array to object
    const columns = mlQueryColumns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))

    // populate views
    const views = []
    await Promise.all(Object.keys(mlViews).map(id => getView(access, id).then(({ name, view }) => {
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
      query: loadedQuery || query,
      views,
      columns,
      isInternal: Object.values(mlViewIsInternal).some(is => is),
      // cost: 1,
    })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to evaluate the execution', 500))
  }
}

// let errors bubble up so the query can be retried
const runExecution = async (executionID) => {
  try {
    const [execution] = await getExecutionMetas({ executionID })
    if (!execution) {
      throw apiError('Invalid execution ID')
    }
    const { whitelabelID, customerID, query, viewIDs, isInternal, status } = execution
    if (status !== STATUS_RUNNING) {
      // don't run unless the status was set to running beforehand
      return
    }
    const access = {
      whitelabel: [whitelabelID],
      customers: [customerID],
      prefix: isInternal ? 'internal' : 'customers',
    }

    // get views
    const views = await Promise.all(viewIDs.map(id => getView(access, id).then(v => v.view)))

    // get query views
    const {
      mlViews,
      mlViewColumns,
      mlViewFdwConnections,
    } = await getQueryViews(access, views, query)

    // insert geo views & joins
    const [
      queryWithGeo,
      viewsWithGeo,
      fdwConnectionsWithGeo,
    ] = insertGeo(access, mlViews, mlViewColumns, mlViewFdwConnections, query)

    // instantiate PG connection to use for fdw + query (must be same)
    // client's application name must be specific to this execution so the pg pid can be
    // readily identified
    const application = `ql-executor-${process.env.STAGE}-${executionID}`
    const pgConnection = newPGClientFromPoolConfig(mlPool, { application_name: application })
    await pgConnection.connect()
    let results
    try {
      // establish fdw connections
      await establishFdwConnections(pgConnection, fdwConnectionsWithGeo)

      // run query
      results = await executeQuery(viewsWithGeo, mlViewColumns, queryWithGeo, { pgConnection })
    } finally {
      pgConnection.end()
    }

    // split results into parts
    const resultsParts = []
    const cacheParts = []
    let partStart = 0
    while (partStart < results.length) {
      const partSize = resultsParts.length ? RESULTS_PART_SIZE : RESULTS_PART_SIZE_FIRST
      const partEnd = Math.min(partStart + partSize, results.length) - 1
      // part will be referred to by its end index relative to the result set
      resultsParts.push(partEnd)
      // persist part to S3
      cacheParts.push(putToS3Cache(
        getExecutionResultsKey(customerID, executionID, resultsParts.length),
        results.slice(partStart, partEnd + 1),
        { gzip: true, json: true, bucket: EXECUTION_BUCKET },
      ))
      partStart = partEnd + 1
    }
    // wait for parts to be persisted to s3
    await Promise.all(cacheParts)

    // update status to succeeded + breakdown of parts
    await updateExecution(
      executionID,
      { status: STATUS_SUCCEEDED, resultsParts },
      { optOutStatuses: [STATUS_CANCELLED] },
    )
  } catch (err) {
    // let the listeners know that the function might be retried
    await updateExecution(
      executionID,
      { status: STATUS_RETRYING },
      { optOutStatuses: [STATUS_CANCELLED] },
    )
    throw err
  }
}

// lambda handler
const executionHandler = ({ execution_id }) => {
  // eslint-disable-next-line radix
  const id = parseInt(execution_id, 10)
  if (Number.isNaN(id)) {
    throw apiError(`Invalid execution ID: ${execution_id}`)
  }
  console.log('execution id', id)
  return runExecution(id)
}

// isRequired flags whether or not 'execution' is a mandatory route/query param
const loadExecution = (isRequired = true) => async (req, _, next) => {
  try {
    if (req.mlQuery) {
      // illegal to populate both req.mlQuery and req.mlExecution
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
    req.mlExecution = execution
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
    const { executionID, customerID, status, viewIDs, columns, resultsParts } = req.mlExecution
    const { results } = req.query
    // attach results
    // TODO: deprecate - retrieve results via results route
    if (['1', 'true'].includes((results || '').toLowerCase()) && status === STATUS_SUCCEEDED) {
      // multi-part
      if (resultsParts) {
        req.mlExecution.results = resultsParts.length
          ? await getExecutionResultsParts(
            customerID,
            executionID,
            resultsParts.map(({ part }) => part),
          )
          : []
      // legacy single part
      } else {
        req.mlExecution.results = await getExecutionResults(
          customerID,
          executionID,
          // { maxSize: 1024 }, // 1MB
        )
      }
    }
    // convert columns from array to object
    req.mlExecution.columns = columns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))

    // populate views
    delete req.mlExecution.viewIDs
    req.mlExecution.views = req.mlExecution.views || []
    await Promise.all(viewIDs.map(id => getView(req.access, id).then(({ name, view }) => {
      view.name = name
      req.mlExecution.views.push(view)
    }).catch((err) => {
      // edge case when view has been unsubscribed or is no longer available
      // soft fail
      req.mlExecution.views.push({
        id,
        error: (err instanceof APIError && err.message) || 'View could not be retrieved',
      })
    })))
    res.json(req.mlExecution)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the execution', 500))
  }
}

const respondWithOrRedirectToExecutionResultsURL = async (req, res, next) => {
  try {
    const { executionID, customerID, status, resultsParts } = req.mlExecution
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
    const { query, results, limit, qhash, chash, status, start, end } = req.query
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
      stat: safeStart,
      end: safeEnd,
      limit: safeLimit,
    })
    if (
      ['1', 'true'].includes((results || '').toLowerCase())
      && executions.length === 1
      && executions[0].status === STATUS_SUCCEEDED
    ) {
      executions[0].results = await getExecutionResults(
        executions[0].customerID,
        executions[0].executionID,
        { maxSize: 1024 }, // 1MB,
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
    const { executionID, status } = req.mlExecution
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
  getExecutionResults,
}

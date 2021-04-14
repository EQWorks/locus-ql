const { knex, mlPool } = require('../util/db')
const { apiError, APIError } = require('../util/api-error')
const { getView, getQueryViews } = require('./views')
const { insertGeo } = require('./geo')
const { executeQuery, establishFdwConnections } = require('./engine')
const { putToS3Cache, getFromS3Cache } = require('./cache')
const { typeToCatMap, CAT_STRING } = require('./type')


const { ML_SCHEMA, ML_EXECUTION_BUCKET } = process.env
const STATUS_QUEUED = 'QUEUED'
const STATUS_SOURCING = 'SOURCING'
const STATUS_RUNNING = 'RUNNING'
const STATUS_RETRYING = 'RETRYING'
const STATUS_SUCCEEDED = 'SUCCEEDED'
const STATUS_FAILED = 'FAILED'

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
      e.cost
    FROM ${ML_SCHEMA}.executions e
    JOIN public.customers c ON c.customerid = e.customer_id
    LEFT JOIN ${ML_SCHEMA}.queries q ON q.query_id = e.query_id AND q.is_active
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
 * Pull the query results from storage
 * @param {number} customerID Customer ID (agency ID)
 * @param {number} executionID Execution ID
 * @param {boolean} [parseFromJson=true] Whether or not to parse the results into an oject
 * @returns {string|Object} Query results
 */
const getExecutionResults = (customerID, executionID, parseFromJson = true) =>
  getFromS3Cache(`${customerID}/${executionID}`, { bucket: ML_EXECUTION_BUCKET, parseFromJson })

/**
 * Creates an execution
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} queryHash Query hash (unique to the query)
 * @param {string} columnHash Column hash (unique to the name/type of the results)
 * @param {Object} query Query object
 * @param {string[]} viewsIDs List of the query views' IDs
 * @param {[string, number][]} columns List of the query columns formatted as [name, pgTypeOID]
 * @param {boolean} isInternal Whether or not the query accesses views restricted to internal users
 * @param {Object.<string, number[]>} dependencies Dynamic views the query depends on (e.g. log or
 * ext views). In the form {dep_type: dep_id[]}
 * @param {number} [queryID] When the execution is tied to a saved query, the id of such query
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @returns {Promise<number>} The execution ID
 */
const createExecution = async (
  customerID,
  queryHash,
  columnHash,
  query,
  viewIDs,
  columns,
  isInternal,
  dependencies,
  queryID,
  knexClient = knex,
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
    customerID,
    queryHash,
    columnHash,
    JSON.stringify(query),
    JSON.stringify(viewIDs),
    JSON.stringify(columns),
    isInternal,
  ]

  // if (dependencies && dependencies.length) {
  if (dependencies && Object.keys(dependencies).length) {
    cols.push('dependencies')
    values.push(JSON.stringify(dependencies))
  }

  if (queryID) {
    cols.push('query_id')
    values.push(queryID)
  }

  const { rows: [{ executionID }] } = await knexClient.raw(`
    INSERT INTO ${ML_SCHEMA}.executions
      (${cols.join(', ')})
    VALUES
      (${cols.map(() => '?').join(', ')})
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
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 */
const updateExecution = async (executionID, { status, queryID }, knexClient = knex) => {
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
  if (!columns.length && !expressions.length) {
    // nothing to update
    return
  }
  await knexClient.raw(`
    UPDATE ${ML_SCHEMA}.executions
    SET ${columns.map(col => `${col} = ?`).concat(expressions).join(', ')}
    WHERE execution_id = ?
  `, [...values, executionID])
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

// extracts async and saved queries and queues them as executions
const queueExecution = async (req, res, next) => {
  try {
    const {
      queryID,
      query: loadedQuery,
      viewIDs: loadedViewIDs,
    } = req.mlQuery || req.mlExecution || {}
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
    const dependencies = sortViewDependencies(mlViewDependencies)
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    // insert into executions
    const executionID = await createExecution(
      access.customers[0],
      mlQueryHash,
      mlQueryColumnHash,
      loadedQuery || query,
      loadedViewIDs || Object.keys(mlViews),
      mlQueryColumns,
      isInternal,
      dependencies,
      queryID,
    )
    res.json({ executionID })
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Query execution could not be started', 500))
  }
}

// let errors bubble up so the query can be restried
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

    // check out PG connection to use for fdw + query (must be same)
    const pgConnection = await mlPool.connect()
    let results
    try {
      // establish fdw connections
      await establishFdwConnections(pgConnection, fdwConnectionsWithGeo)

      // run query
      results = await executeQuery(viewsWithGeo, mlViewColumns, queryWithGeo, { pgConnection })
    } finally {
      pgConnection.release()
    }

    // persist to S3
    await putToS3Cache(
      `${customerID}/${executionID}`,
      results,
      { gzip: true, json: true, bucket: ML_EXECUTION_BUCKET },
    )

    // update status to succeeded
    await updateExecution(executionID, { status: STATUS_SUCCEEDED })
  } catch (err) {
    // let the listeners know that the function muight be retried
    await updateExecution(executionID, { status: STATUS_RETRYING })
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
      throw apiError('Invalid query ID', 404)
    }
    // attach to req
    req.mlExecution = execution
    // set customer to that of the execution
    req.access = {
      ...access,
      whitelabel: [execution.whitelabelID],
      customers: [execution.customerID],
    }
    next()
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to load the execution', 500))
  }
}

const respondWithExecution = async (req, res, next) => {
  try {
    const { executionID, customerID, status, viewIDs, columns } = req.mlExecution
    const { results } = req.query
    // attach results
    if (['1', 'true'].includes((results || '').toLowerCase()) && status === STATUS_SUCCEEDED) {
      req.mlExecution.results = await getExecutionResults(customerID, executionID)
    }

    // convert columns from array to object
    req.mlExecution.columns = columns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))

    // populate views
    delete req.mlExecution.viewIDs
    await Promise.all(viewIDs.map(id => getView(req.access, id).then(({ name, view }) => {
      req.mlExecution.views = req.mlExecution.views || []
      view.name = name
      req.mlExecution.views.push(view)
    })))
    res.json(req.mlExecution)
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to retrieve the execution', 500))
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
      acc.push(...viewIDs.map((id) => {
        // memoize promises
        if (!(id in viewMemo)) {
          viewMemo[id] = getView(req.access, id)
        }
        return viewMemo[id].then(({ name, view }) => {
          e.views = e.views || []
          view.name = name
          e.views.push(view)
        })
      }))
      return acc
    }, []))
    res.json(executions)
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to retrieve the executions', 500))
  }
}

// to test out handler (replace ID)
// executionHandler({ execution_id: '205' }).then(() => console.log('ml execution done'))

module.exports = {
  createExecution,
  updateExecution,
  queueExecution,
  executionHandler,
  loadExecution,
  respondWithExecution,
  listExecutions,
}

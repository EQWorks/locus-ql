const { createHash } = require('crypto')

const { knex } = require('../util/db')
const apiError = require('../util/api-error')
const { getView, getQueryViews } = require('./views')
const { executeQuery } = require('./engine')
const { putToS3Cache, getFromS3Cache, EXECUTION_BUCKET } = require('./cache')


const { ML_SCHEMA } = process.env
const STATUS_QUEUED = 'QUEUED'
const STATUS_SOURCING = 'SOURCING'
const STATUS_RUNNING = 'RUNNING'
const STATUS_RETRYING = 'RETRYING'
const STATUS_SUCCEEDED = 'SUCCEEDED'
const STATUS_FAILED = 'FAILED'

const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

/**
 * Returns a unique hash per query markup
 * The hash can be used to version queries
 * @param {Object} query Query markup
 * @returns {string} Query hash
 */
const getQueryHash = query => createHash('sha256').update(JSON.stringify(query)).digest('base64')

/**
 * Returns an array of execution metas based on the supplied filters
 * @param {Object} [filters]
 * @param {number} [filters.executionID] Execution ID
 * @param {-1|number[]} [filters.whitelabelIDs] Array of whitelabel IDs (agency ID)
 * @param {-1|number[]} [filters.customerIDs] Array of customer IDs (agency ID)
 * @param {number} [filters.queryID] Saved query ID
 * @param {number} [filters.queryHash] Query hash
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
      e.query_hash AS "queryHash",
      e.status,
      e.status_ts AS "statusTS",
      q.query_id AS "queryID",
      e.markup,
      e.is_internal AS "isInternal",
      e.query_id <> q.query_id AS "isOrphaned",
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
      ${status ? 'AND e.status = :status' : ''}
      ${start ? 'AND e.status_ts >= to_timestamp(:start)' : ''}
      ${end ? 'AND e.status_ts <= to_timestamp(:end)' : ''}
      ${hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ORDER BY 6 DESC
    ${limit ? 'LIMIT :limit' : ''}
  `, { executionID, whitelabelIDs, customerIDs, queryID, queryHash, status, start, end, limit })
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
  getFromS3Cache(`${customerID}/${executionID}`, { bucket: EXECUTION_BUCKET, parseFromJson })

/**
 * Creates an execution
 * @param {number} customerID Customer ID (agency ID)
 * @param {{ query: Object, views: string[] }} markup The query object along with a list
 * of the query views
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
  markup,
  isInternal,
  dependencies,
  queryID,
  knexClient = knex,
) => {
  const queryHash = getQueryHash(markup.query)
  const columns = ['customer_id', 'query_hash', 'markup', 'is_internal']
  const values = [customerID, queryHash, JSON.stringify(markup), isInternal]

  // if (dependencies && dependencies.length) {
  if (dependencies && Object.keys(dependencies).length) {
    columns.push('dependencies')
    values.push(JSON.stringify(dependencies))
  }

  if (queryID) {
    columns.push('query_id')
    values.push(queryID)
  }

  const { rows: [{ executionID }] } = await knexClient.raw(`
    INSERT INTO ${ML_SCHEMA}.executions
      (${columns.join(', ')})
    VALUES
      (${columns.map(() => '?').join(', ')})
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
    const { queryID, markup: reqMarkup } = req.mlQuery || {}
    const { query } = req.body
    const { access, mlViews, mlViewDependencies, mlViewIsInternal } = req
    const dependencies = sortViewDependencies(mlViewDependencies)
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = reqMarkup || { query, viewIDs: Object.keys(mlViews) }
    // insert into executions
    const executionID = await createExecution(
      access.customers[0],
      markup,
      isInternal,
      dependencies,
      queryID,
    )
    res.json({ executionID })
  } catch (err) {
    next(err)
  }
}

// executes the query synchronously
// const runQuery = async (req, res, next) => {
//   const { query } = req.body
//   // eslint-disable-next-line radix
//   const cacheMaxAge = parseInt(req.query.cache, 10) || undefined
//   try {
//     const results = await executeQuery(req.mlViews, req.mlViewColumns, query, cacheMaxAge)
//     return res.status(200).json(results)
//   } catch (err) {
//     next(err)
//   }
// }

// let errors bubble up so the query can be restried
const runExecution = async (executionID) => {
  try {
    const [execution] = await getExecutionMetas({ executionID })
    if (!execution) {
      throw apiError('Invalid execution ID')
    }
    const { whitelabelID, customerID, markup, isInternal, status } = execution
    if (![STATUS_RUNNING, STATUS_RETRYING].includes(status)) {
      // only run queued or failed queries
      return
    }
    const access = {
      whitelabel: [whitelabelID],
      customers: [customerID],
      prefix: isInternal ? 'internal' : 'customers',
    }

    const { query, viewIDs } = markup

    // get views
    const views = await Promise.all(viewIDs.map(id => getView(access, id).then(v => v.view)))

    // get query views
    const { mlViews, mlViewColumns } = await getQueryViews(access, views, query)

    // run query
    const results = await executeQuery(mlViews, mlViewColumns, query)

    // persist to S3
    await putToS3Cache(
      `${customerID}/${executionID}`,
      results,
      { gzip: true, json: true, bucket: EXECUTION_BUCKET },
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
const executionHandler = ({ executionID }) => runExecution(executionID)

// isRequired flags whether or not 'query' is a mandatory route/query param
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
    next(err)
  }
}

/*
Query params:
 - results = 1|true
*/
const respondWithExecution = async (req, res, next) => {
  try {
    const { executionID, customerID, status, markup } = req.mlExecution
    const { results } = req.query
    // attach results
    if (['1', 'true'].includes(results) && status === STATUS_SUCCEEDED) {
      req.mlExecution.results = await getExecutionResults(customerID, executionID)
    }
    // populate views
    const { viewIDs } = markup
    delete markup.viewIDs
    await Promise.all(viewIDs.map(id => getView(req.access, id).then((v) => {
      markup.views = markup.views || []
      markup.views.push(v.view)
    })))
    res.json(req.mlExecution)
  } catch (err) {
    next(err)
  }
}

/*
Query params:
 - results = 1|true
*/
// const getExecution = async (req, res, next) => {
//   try {
//     const { id } = req.params
//     // eslint-disable-next-line radix
//     const executionID = parseInt(id, 10)
//     if (Number.isNaN(executionID)) {
//       throw apiError('Invalid execution ID')
//     }
//     const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
//     const hideInternal = !isInternalUser(prefix)
//     const [execution] = await getExecutionMetas({
//       executionID,
//       whitelabelIDs,
//       customerIDs,
//       hideInternal,
//     })
//     if (!execution) {
//       throw apiError('Invalid execution ID', 404)
//     }
//     const { customerID, status, markup } = execution
//     const { results } = req.query
//     if (['1', 'true'].includes(results) && status === STATUS_SUCCEEDED) {
//       execution.results = await getExecutionResults(customerID, executionID)
//     }
//     // populate views
//     const { viewIDs } = markup
//     delete markup.viewIDs
//     await Promise.all(viewIDs.map(id => getView(req.access, id).then((v) => {
//       markup.views = markup.views || []
//       markup.views.push(v.view)
//     })))
//     res.json(execution)
//   } catch (err) {
//     next(err)
//   }
// }

/*
Query params:
 - query = ID
 - results = 1|true -> apply if executions.length === 1
 - limit = max # of executions
 - hash
 - status
 - start
 - end
*/
const listExecutions = async (req, res, next) => {
  try {
    const { query, results, limit, hash, status, start, end } = req.query
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
      queryHash: hash,
      status: safeStatus,
      stat: safeStart,
      end: safeEnd,
      limit: safeLimit,
    })
    if (
      ['1', 'true'].includes(results)
      && executions.length === 1
      && executions[0].status === STATUS_SUCCEEDED
    ) {
      executions[0].results = await getExecutionResults(
        executions[0].customerID,
        executions[0].executionID,
      )
    }
    // populate views
    const viewMemo = {}
    await Promise.all(executions.reduce((acc, { markup }) => {
      const { viewIDs } = markup
      delete markup.viewIDs
      acc.push(...viewIDs.map((id) => {
        // memoize promises
        if (!(id in viewMemo)) {
          viewMemo[id] = getView(req.access, id)
        }
        return viewMemo[id].then(({ view }) => {
          markup.views = markup.views || []
          markup.views.push(view)
        })
      }))
      return acc
    }, []))
    res.json(executions)
  } catch (err) {
    next(err)
  }
}

// to test out handler (replace ID)
// executionHandler({ executionID: 5 }).then(() => console.log('ml execution done'))

module.exports = {
  createExecution,
  updateExecution,
  queueExecution,
  // runQuery,
  executionHandler,
  // getExecution,
  loadExecution,
  respondWithExecution,
  listExecutions,
  getQueryHash,
}

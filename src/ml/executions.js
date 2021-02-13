const { createHash } = require('crypto')

const { knex } = require('../util/db')
const apiError = require('../util/api-error')
const { getView, getQueryViews } = require('./views')
const { execute } = require('./engine')
const { putToS3Cache, getFromS3Cache } = require('./cache')


const { ML_SCHEMA } = process.env
const STATUS_QUEUED = 'QUEUED'
const STATUS_SOURCING = 'SOURCING'
const STATUS_RUNNING = 'RUNNING'
const STATUS_RETRYING = 'RETRYING'
const STATUS_SUCCEEDED = 'SUCCEEDED'
const EXECUTION_BUCKET = 'ml-execution-cache-dev'

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
 * @param {boolean} [filters.hideInternal] Whether or to filter out queries using internal fields
 * @returns {Promise<Array>}
 */
const getExecutionMetas = async ({
  executionID,
  whitelabelIDs,
  customerIDs,
  queryID,
  hideInternal,
} = {}) => {
  const { rows } = await knex.raw(`
    SELECT
      e.execution_id AS "executionID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      e.query_hash AS "queryHash",
      e.status,
      e.status_ts AS "statusTS",
      e.query_id AS "queryID",
      e.markup,
      e.is_internal AS "isInternal"
    FROM ${ML_SCHEMA}.executions e
    JOIN public.customers c ON c.customerid = e.customer_id
    WHERE
      TRUE
      ${executionID ? 'AND e.execution_id = :executionID' : ''}
      ${whitelabelIDs && whitelabelIDs !== -1 ? 'AND c.whitelabelid = ANY(:whitelabelIDs)' : ''}
      ${customerIDs && customerIDs !== -1 ? 'AND e.customer_id = ANY(:customerIDs)' : ''}
      ${queryID ? 'AND e.query_id = :queryID)' : ''}
      ${hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ${executionID ? 'LIMIT 1' : ''}
  `, { executionID, whitelabelIDs, customerIDs, queryID })
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
 * @param {Record<string, number[]>} dependencies Dynamic views the query depends on (e.g. log or
 * ext views). In the form {dep_type: dep_id[]}
 * @param {number} [queryID] When the execution is tied to a saved query, the id of such query
 */
const createExecution = async (customerID, markup, isInternal, dependencies, queryID) => {
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

  const { rows: [{ executionID }] } = await knex.raw(`
    INSERT INTO ${ML_SCHEMA}.executions
      (${columns.join(', ')})
    VALUES
      (${columns.map(() => '?').join(', ')})
    RETURNING execution_id AS "executionID"
  `, values)

  return executionID
}

/**
 * Updates the excution's status
 * @param {number} executionID Execution ID
 * @param {string} status New status
 */
const updateExecutionStatus = async (executionID, status) => {
  await knex.raw(`
    UPDATE ${ML_SCHEMA}.executions
    SET
      status = ?,
      status_ts = now()
    WHERE execution_id = ?
  `, [status, executionID])
}

/**
 * Sorts view dependencies by type and removes duplicates
 * @param {Record<string, [string, number][]>} viewDependencies Dependencies per views
 * @returns {Record<string, number[]>} Unique dependencies per type
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
  const { query } = req.body
  const { access, mlViews, mlViewDependencies, mlViewIsInternal } = req
  try {
    const dependencies = sortViewDependencies(mlViewDependencies)
    if (!Object.keys(dependencies).length) {
      next()
    }
    // this is an async query
    const { whitelabel, customers } = access
    if (
      !Array.isArray(whitelabel)
      || whitelabel.length !== 1
      || !Array.isArray(customers)
      || customers.length !== 1
    ) {
      throw apiError('Failed to submit an execution: customer cannot be identified')
    }

    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = { query, viewIDs: Object.keys(mlViews) }
    // insert into executions
    const executionID = await createExecution(customers[0], markup, isInternal, dependencies)
    res.json({ executionID })
  } catch (err) {
    return next(err)
  }
}

// executes the query synchronously
const runQuery = async (req, res, next) => {
  const { query } = req.body
  // eslint-disable-next-line radix
  const cacheMaxAge = parseInt(req.query.cache, 10) || undefined
  try {
    const results = await execute(req.mlViews, req.mlViewColumns, query, cacheMaxAge)
    return res.status(200).json(results)
  } catch (err) {
    return next(err)
  }
}

// let errors bubble up so the query can be restried
const runExecution = async (executionID) => {
  try {
    const [execution] = await getExecutionMetas({ executionID })
    if (!execution) {
      throw apiError('Invalid execution ID')
    }
    const { whitelabelID, customerID, markup, isInternal, status } = execution
    if (status !== STATUS_RUNNING) {
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
    const results = await execute(mlViews, mlViewColumns, query)

    // persist to S3
    await putToS3Cache(
      `${customerID}/${executionID}`,
      results,
      { gzip: true, json: true, bucket: EXECUTION_BUCKET },
    )

    // update status to succeeded
    await updateExecutionStatus(executionID, STATUS_SUCCEEDED)
  } catch (err) {
    // let the listeners know that the function muight be retried
    await updateExecutionStatus(executionID, STATUS_RETRYING)
    throw err
  }
}

const getExecution = async (req, res, next) => {
  try {
    const { id } = req.params
    // eslint-disable-next-line radix
    const executionID = parseInt(id, 10)
    if (Number.isNaN(executionID)) {
      throw apiError('Invalid execution ID')
    }
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
    const hideInternal = !isInternalUser(prefix)
    const [execution] = await getExecutionMetas({ executionID, whitelabelIDs, customerIDs, hideInternal })
    if (!execution) {
      throw apiError('Invalid execution ID')
    }
    const { customerID, status } = execution
    const { results } = req.query
    if (['1', 'true'].includes(results) && status === STATUS_SUCCEEDED) {
      execution.results = await getExecutionResults(customerID, executionID)
    }
    // might need to repopulate views (as views in markup is array of viewID's)
    res.json(execution)
  } catch (err) {
    next(err)
  }
}

const listExecutions = async (req, res, next) => {
  try {
    const { query } = req.query
    let queryID
    if (query) {
      // eslint-disable-next-line radix
      queryID = parseInt(query, 10)
      if (Number.isNaN(queryID)) {
        throw apiError('Invalid query ID')
      }
    }
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
    const hideInternal = !isInternalUser(prefix)
    const executions = await getExecutionMetas({ whitelabelIDs, customerIDs, hideInternal, queryID })
    res.json(executions)
  } catch (err) {
    next(err)
  }
}

module.exports = {
  createExecution,
  queueExecution,
  runQuery,
  runExecution,
  getExecution,
  listExecutions,
}

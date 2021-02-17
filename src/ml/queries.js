const { createHash } = require('crypto')


const { knex } = require('../util/db')
const apiError = require('../util/api-error')
const { getView } = require('./views')
const { getKnexQuery } = require('./engine')


const { ML_SCHEMA } = process.env

const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

/**
 * Returns a unique hash per query markup
 * The hash can be used to version queries
 * @param {Object} query Query markup
 * @returns {string} Query hash
 */
const getQueryHash = query => createHash('sha256').update(JSON.stringify(query)).digest('base64')

/**
 * Returns an array of query metas based on the supplied filters
 * @param {Object} [filters]
 * @param {number} [filters.queryID] Saved query ID
 * @param {-1|number[]} [filters.whitelabelIDs] Array of whitelabel IDs (agency ID)
 * @param {-1|number[]} [filters.customerIDs] Array of customer IDs (agency ID)
 * @param {number} [filters.executionID] Execution ID
 * @param {number} [filters.queryHash] Query hash
 * @param {boolean} [filters.hideInternal=false] Whether or not to filter out queries
 * using internal fields
 * @param {boolean} [filters.showExecutions=false] Whether or not to append the list of
 * executions to the query object
 * @returns {Promise<Array>}
 */
const getQueryMetas = async ({
  queryID,
  whitelabelIDs,
  customerIDs,
  executionID,
  queryHash,
  hideInternal = false,
  showExecutions = false,
} = {}) => {
  const { rows } = await knex.raw(`
    SELECT
      q.query_id AS "queryID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      q.query_hash AS "queryHash",
      q.name,
      q.description,
      q.markup
      ${showExecutions ? `, array_agg(
        json_build_object(
          'executionID', e.execution_id,
          'queryHash', e.query_hash,
          'status', e.status,
          'statusTS', e.status_ts,
          'isInternal', e.is_internal
        ) ORDER BY e.execution_id DESC
      ) AS "executions"` : ''}
    FROM ${ML_SCHEMA}.queries q
    JOIN public.customers c ON c.customerid = q.customer_id
    ${showExecutions || executionID ? `LEFT JOIN ${ML_SCHEMA}.executions e USING (query_id)` : ''}
    WHERE
      TRUE
      ${queryID ? 'AND q.query_id = :queryID' : ''}
      ${whitelabelIDs && whitelabelIDs !== -1 ? 'AND c.whitelabelid = ANY(:whitelabelIDs)' : ''}
      ${customerIDs && customerIDs !== -1 ? 'AND c.customerid = ANY(:customerIDs)' : ''}
      ${executionID ? 'AND e.execution_id = :executionID' : ''}
      ${queryHash ? 'AND q.query_hash = :queryHash' : ''}
      ${hideInternal ? 'AND q.is_internal <> TRUE' : ''}
      ${showExecutions && hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ${showExecutions ? 'GROUP BY 1, 2, 3, 4, 5, 6, 7' : ''}
  ORDER BY 1 DESC
  `, { queryID, whitelabelIDs, customerIDs, executionID, queryHash })
  return rows
}

/**
 * Creates a query
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} name Query name
 * @param {Object} markup The query object along with a list
 * of the query views
 * @param {Object} markup.query Query object
 * @param {string[]} markup.viewsIDs List of the query views' IDs
 * @param {boolean} isInternal Whether or not the query accesses views restricted to internal users
 * @param {string} [description] Query description
 * @returns {Promise<number>} The query ID
 */
const createQuery = async (customerID, name, markup, isInternal, description) => {
  const queryHash = getQueryHash(markup.query)
  const columns = ['customer_id', 'query_hash', 'name', 'markup', 'is_internal']
  const values = [customerID, queryHash, name, JSON.stringify(markup), isInternal]

  if (description) {
    columns.push('description')
    values.push(description)
  }

  const { rows: [{ queryID }] } = await knex.raw(`
    INSERT INTO ${ML_SCHEMA}.queries
      (${columns.join(', ')})
    VALUES
      (${columns.map(() => '?').join(', ')})
    RETURNING query_id AS "queryID"
  `, values)

  return queryID
}

/**
 * Updates a query based on its id
 * @param {number} queryID Query ID
 * @param {Object} updates
 * @param {string} [updates.name] Query name
 * @param {Object} [updates.markup] The query object along with a list
 * of the query views
 * @param {Object} updates.markup.query Query object
 * @param {string[]} updates.markup.viewsIDs List of the query views' IDs
 * @param {boolean} [updates.isInternal] Whether or not the query accesses views restricted to
 * internal users
 * @param {string} [updates.description] Query description
 */
const updateQuery = async (queryID, { name, markup, isInternal, description }) => {
  const columns = []
  const values = []

  if (name) {
    columns.push('name')
    values.push(name)
  }

  if (markup) {
    columns.push('query_hash', 'markup')
    values.push(getQueryHash(markup.query), markup)
  }

  if (isInternal !== undefined) {
    columns.push('is_internal')
    values.push(isInternal)
  }

  if (description) {
    columns.push('description')
    values.push(description)
  }

  if (!columns.length) {
    // nothing to update
    return
  }

  await knex.raw(`
    UPDATE ${ML_SCHEMA}.queries
    SET (${columns.join(', ')}) = (${columns.map(() => '?').join(', ')})
    WHERE query_id = ?
  `, [...values, queryID])
}


const postQuery = async (req, res, next) => {
  const { name, description, query } = req.body
  const { access, mlViews, mlViewColumns, mlViewIsInternal } = req

  try {
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    // validate query
    // if no error then query was parsed successfully
    getKnexQuery(mlViews, mlViewColumns, query)

    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = { query, viewIDs: Object.keys(mlViews) }
    const queryID = await createQuery(access.customers[0], name, markup, isInternal, description)
    res.json({ queryID })
  } catch (err) {
    return next(err)
  }
}

const putQuery = async (req, res, next) => {
  const { queryID } = req.mlQuery
  const { name, description, query } = req.body
  const { mlViews, mlViewColumns, mlViewIsInternal } = req
  try {
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    // validate query
    // if no error then query was parsed successfully
    getKnexQuery(mlViews, mlViewColumns, query)

    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = { query, viewIDs: Object.keys(mlViews) }
    await updateQuery(queryID, { name, markup, isInternal, description })
    res.json({ queryID })
  } catch (err) {
    return next(err)
  }
}

const loadQuery = async (req, _, next) => {
  try {
    const query = req.params.id || req.query.query
    // eslint-disable-next-line radix
    const queryID = parseInt(query, 10)
    if (Number.isNaN(queryID)) {
      throw apiError('Invalid query ID')
    }
    const { access } = req
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = access
    const hideInternal = !isInternalUser(prefix)
    const [{ markup }] = await getQueryMetas({
      queryID,
      whitelabelIDs,
      customerIDs,
      hideInternal,
    })
    if (!query) {
      throw apiError('Invalid query ID', 404)
    }

    // get views
    const views = await Promise.all(markup.viewIDs.map(id => getView(access, id).then(v => v.view)))

    req.mlQuery = {
      queryID,
      body: { query: markup.query, views },
    }
    next()
  } catch (err) {
    next(err)
  }
}


const getQuery = async (req, res, next) => {
  try {
    const { id } = req.params
    // eslint-disable-next-line radix
    const queryID = parseInt(id, 10)
    if (Number.isNaN(queryID)) {
      throw apiError('Invalid query ID')
    }
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
    const hideInternal = !isInternalUser(prefix)
    const [query] = await getQueryMetas({
      queryID,
      whitelabelIDs,
      customerIDs,
      hideInternal,
      showExecutions: true,
    })
    if (!query) {
      throw apiError('Invalid query ID', 404)
    }
    res.json(query)
  } catch (err) {
    next(err)
  }
}

/*
Query params:
 - execution = ID
 - hash
*/
const listQueries = async (req, res, next) => {
  try {
    const { execution, hash } = req.query
    // sanitize query params
    let executionID
    if (execution) {
      // eslint-disable-next-line radix
      executionID = parseInt(execution, 10)
      if (Number.isNaN(executionID)) {
        throw apiError(`Invalid execution ID: ${execution}`)
      }
    }
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
    const hideInternal = !isInternalUser(prefix)
    const queries = await getQueryMetas({
      whitelabelIDs,
      customerIDs,
      hideInternal,
      executionID,
      queryHash: hash,
    })
    res.json(queries)
  } catch (err) {
    next(err)
  }
}

module.exports = {
  createQuery,
  postQuery,
  putQuery,
  loadQuery,
  getQuery,
  listQueries,
  getQueryHash,
}

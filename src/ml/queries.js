const { knex } = require('../util/db')
const apiError = require('../util/api-error')
const { getView } = require('./views')
const { updateExecution, getQueryHash } = require('./executions')


const { ML_SCHEMA } = process.env

const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

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
        ) ORDER BY e.status_ts DESC
      ) AS "executions"` : ''}
    FROM ${ML_SCHEMA}.queries q
    JOIN public.customers c ON c.customerid = q.customer_id
    ${showExecutions || executionID ? `LEFT JOIN ${ML_SCHEMA}.executions e USING (query_id)` : ''}
    WHERE
      q.is_active
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
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @returns {Promise<number>} The query ID
 */
const createQuery = async (
  customerID,
  name,
  markup,
  isInternal,
  description,
  knexClient = knex,
) => {
  const queryHash = getQueryHash(markup.query)
  const columns = ['customer_id', 'query_hash', 'name', 'markup', 'is_internal']
  const values = [customerID, queryHash, name, JSON.stringify(markup), isInternal]

  if (description) {
    columns.push('description')
    values.push(description)
  }

  const { rows: [{ queryID }] } = await knexClient.raw(`
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
 * @param {boolean} [updates.isActive] Active status
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 */
const updateQuery = async (
  queryID,
  { name, markup, isInternal, description, isActive },
  knexClient = knex,
) => {
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
  if (isActive !== undefined) {
    columns.push('is_active')
    values.push(isActive)
  }
  if (!columns.length) {
    // nothing to update
    return
  }
  await knexClient.raw(`
    UPDATE ${ML_SCHEMA}.queries
    SET ${columns.map(col => `${col} = ?`).join(', ')}
    WHERE query_id = ?
  `, [...values, queryID])
}

const postQuery = async (req, res, next) => {
  try {
    const { name, description, query } = req.body
    const { access: { customers }, mlViews, mlViewIsInternal } = req
    const { markup: reqMarkup } = req.mlQuery || req.mlExecution || {}
    const { executionID, queryID: executionQueryID, isOrphaned } = req.mlExecution || {}
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = reqMarkup || { query, viewIDs: Object.keys(mlViews) }

    // create query + update execution (as applicable) in transaction
    const queryID = await knex.transaction(async (trx) => {
      const queryID = await createQuery(customers[0], name, markup, isInternal, description, trx)
      // if execution supplied, attach it to the created query if not already attached to a query
      if (executionID && !executionQueryID && !isOrphaned) {
        await updateExecution(executionID, { queryID }, trx)
      }
      return queryID
    })

    res.json({ queryID })
  } catch (err) {
    next(err)
  }
}

const putQuery = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    const { name, description, query } = req.body
    const { mlViews, mlViewIsInternal } = req
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = { query, viewIDs: Object.keys(mlViews) }
    await updateQuery(queryID, { name, markup, isInternal, description })
    res.json({ queryID })
  } catch (err) {
    next(err)
  }
}

const deleteQuery = async (req, res, next) => {
  try {
    const { queryID } = req.mlQuery
    await updateQuery(queryID, { isActive: false })
    res.json({ queryID })
  } catch (err) {
    next(err)
  }
}

// isRequired flags whether or not 'query' is a mandatory route/query param
const loadQuery = (isRequired = true) => async (req, _, next) => {
  try {
    const id = req.params.id || req.query.query
    // eslint-disable-next-line radix
    const queryID = parseInt(id, 10)
    if (Number.isNaN(queryID)) {
      if (isRequired) {
        throw apiError('Invalid query ID')
      }
      return next()
    }
    const { access } = req
    const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = access
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
    // attach to req
    req.mlQuery = query
    // set customer to that of the query
    req.access = {
      ...access,
      whitelabel: [query.whitelabelID],
      customers: [query.customerID],
    }
    next()
  } catch (err) {
    next(err)
  }
}

const respondWithQuery = async (req, res, next) => {
  try {
    const { markup } = req.mlQuery
    // populate views (instead of viewIDs)
    const { viewIDs } = markup
    delete markup.viewIDs
    await Promise.all(viewIDs.map(id => getView(req.access, id).then((v) => {
      markup.views = markup.views || []
      markup.views.push(v.view)
    })))
    res.json(req.mlQuery)
  } catch (err) {
    next(err)
  }
}

// const getQuery = async (req, res, next) => {
//   try {
//     const { id } = req.params
//     // eslint-disable-next-line radix
//     const queryID = parseInt(id, 10)
//     if (Number.isNaN(queryID)) {
//       throw apiError('Invalid query ID')
//     }
//     const { whitelabel: whitelabelIDs, customers: customerIDs, prefix } = req.access
//     const hideInternal = !isInternalUser(prefix)
//     const [query] = await getQueryMetas({
//       queryID,
//       whitelabelIDs,
//       customerIDs,
//       hideInternal,
//       showExecutions: true,
//     })
//     if (!query) {
//       throw apiError('Invalid query ID', 404)
//     }
//     res.json(query)
//   } catch (err) {
//     next(err)
//   }
// }

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
    // populate views
    const viewMemo = {}
    await Promise.all(queries.reduce((acc, { markup }) => {
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
    res.json(queries)
  } catch (err) {
    next(err)
  }
}

module.exports = {
  createQuery,
  postQuery,
  putQuery,
  deleteQuery,
  loadQuery,
  respondWithQuery,
  // getQuery,
  listQueries,
}

const { knex } = require('../util/db')
const apiError = require('../util/api-error')
const { getView } = require('./views')
const { updateExecution } = require('./executions')


const { ML_SCHEMA } = process.env

const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

/**
 * Returns an array of query metas based on the supplied filters
 * @param {Object} [filters]
 * @param {number} [filters.queryID] Saved query ID
 * @param {-1|number[]} [filters.whitelabelIDs] Array of whitelabel IDs (agency ID)
 * @param {-1|number[]} [filters.customerIDs] Array of customer IDs (agency ID)
 * @param {number} [filters.executionID] Execution ID
 * @param {string} [filters.queryHash] Query hash (unique to the query)
 * @param {string} [filters.columnHash] Column hash (unique to the name/type of the results)
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
  columnHash,
  hideInternal = false,
  showExecutions = false,
} = {}) => {
  const { rows } = await knex.raw(`
    SELECT
      q.query_id AS "queryID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      q.query_hash AS "queryHash",
      q.column_hash AS "columnHash",
      q.name,
      q.description,
      q.markup,
      q.columns,
      q.is_internal AS "isInternal"
      ${showExecutions ? `, coalesce(
        array_agg(
          json_build_object(
            'executionID', e.execution_id,
            'queryHash', e.query_hash,
            'status', e.status,
            'statusTS', e.status_ts,
            'isInternal', e.is_internal
          ) ORDER BY e.execution_id DESC
        ) FILTER (WHERE e.execution_id IS NOT NULL),
        '{}'
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
      ${columnHash ? 'AND q.column_hash = :columnHash' : ''}
      ${hideInternal ? 'AND q.is_internal <> TRUE' : ''}
      ${showExecutions && hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ${showExecutions ? 'GROUP BY 1, 2, 3, 4, 5, 6, 7' : ''}
  ORDER BY 1 DESC
  `, { queryID, whitelabelIDs, customerIDs, executionID, queryHash, columnHash })
  return rows
}

/**
 * Creates a query
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} queryHash Query hash (unique to the query)
 * @param {string} columnHash Column hash (unique to the name/type of the results)
 * @param {string} name Query name
 * @param {Object} markup The query object along with a list
 * of the query views
 * @param {Object} markup.query Query object
 * @param {string[]} markup.viewsIDs List of the query views' IDs
 * @param {[string, string][]} columns List of the query columns formatted as [name, category]
 * @param {boolean} isInternal Whether or not the query accesses views restricted to internal users
 * @param {string} [description] Query description
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @returns {Promise<number>} The query ID
 */
const createQuery = async (
  customerID,
  queryHash,
  columnHash,
  name,
  markup,
  columns,
  isInternal,
  description,
  knexClient = knex,
) => {
  const cols = [
    'customer_id',
    'query_hash',
    'column_hash',
    'name',
    'markup',
    'columns',
    'is_internal',
  ]
  const values = [
    customerID,
    queryHash,
    columnHash,
    name,
    JSON.stringify(markup),
    JSON.stringify(columns),
    isInternal,
  ]

  if (description) {
    cols.push('description')
    values.push(description)
  }

  const { rows: [{ queryID }] } = await knexClient.raw(`
    INSERT INTO ${ML_SCHEMA}.queries
      (${cols.join(', ')})
    VALUES
      (${cols.map(() => '?').join(', ')})
    RETURNING query_id AS "queryID"
  `, values)

  return queryID
}

/**
 * Updates a query based on its id
 * @param {number} queryID Query ID
 * @param {Object} updates
 * @param {string} [updates.queryHash] Query hash (unique to the query)
 * @param {string} [updates.columnHash] Column hash (unique to the name/type of the results)
 * @param {string} [updates.name] Query name
 * @param {Object} [updates.markup] The query object along with a list
 * of the query views
 * @param {Object} updates.markup.query Query object
 * @param {string[]} updates.markup.viewsIDs List of the query views' IDs
 * @param {[string, string][]} [updates.columns] List of the query columns
 * formatted as [name, category]
 * @param {boolean} [updates.isInternal] Whether or not the query accesses views restricted to
 * internal users
 * @param {string} [updates.description] Query description
 * @param {boolean} [updates.isActive] Active status
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 */
const updateQuery = async (
  queryID,
  { queryHash, columnHash, name, markup, columns, isInternal, description, isActive },
  knexClient = knex,
) => {
  const cols = []
  const values = []
  if (queryHash) {
    cols.push('query_hash')
    values.push(queryHash)
  }
  if (columnHash) {
    cols.push('column_hash')
    values.push(columnHash)
  }
  if (name) {
    cols.push('name')
    values.push(name)
  }
  if (markup) {
    cols.push('markup')
    values.push(markup)
  }
  if (columns) {
    cols.push('columns')
    values.push(columns)
  }
  if (isInternal !== undefined) {
    cols.push('is_internal')
    values.push(isInternal)
  }
  if (description) {
    cols.push('description')
    values.push(description)
  }
  if (isActive !== undefined) {
    cols.push('is_active')
    values.push(isActive)
  }
  if (!cols.length) {
    // nothing to update
    return
  }
  await knexClient.raw(`
    UPDATE ${ML_SCHEMA}.queries
    SET ${cols.map(col => `${col} = ?`).join(', ')}
    WHERE query_id = ?
  `, [...values, queryID])
}

const postQuery = async (req, res, next) => {
  try {
    const { name, description, query } = req.body
    const {
      access: { customers },
      mlViews,
      mlViewIsInternal,
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
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
      const queryID = await createQuery(
        customers[0],
        mlQueryHash,
        mlQueryColumnHash,
        name,
        markup,
        mlQueryColumns,
        isInternal,
        description,
        trx,
      )
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
    const { mlViews, mlViewIsInternal, mlQueryHash, mlQueryColumnHash, mlQueryColumns } = req
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(mlViewIsInternal).some(is => is)
    const markup = { query, viewIDs: Object.keys(mlViews) }
    await updateQuery(queryID, {
      name,
      queryHash: mlQueryHash,
      columnHash: mlQueryColumnHash,
      markup,
      columns: mlQueryColumns,
      isInternal,
      description,
    })
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
    const { markup, columns } = req.mlQuery

    // convert columns from array to object
    req.mlQuery.columns = columns.map(([name, category]) => ({ name, category }))

    // populate views (instead of viewIDs)
    const { viewIDs } = markup
    delete markup.viewIDs
    await Promise.all(viewIDs.map(id => getView(req.access, id).then(({ name, view }) => {
      markup.views = markup.views || []
      view.name = name
      markup.views.push(view)
    })))
    res.json(req.mlQuery)
  } catch (err) {
    next(err)
  }
}

const listQueries = async (req, res, next) => {
  try {
    const { execution, qhash, chash } = req.query
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
      executionID,
      queryHash: qhash,
      columnHash: chash,
      hideInternal,
      showExecutions: true,
    })
    // prepare response
    const viewMemo = {}
    await Promise.all(queries.reduce((acc, q) => {
      const { markup, columns } = q

      // convert columns from array to object
      q.columns = columns.map(([name, category]) => ({ name, category }))

      // populate views
      const { viewIDs } = markup
      delete markup.viewIDs
      acc.push(...viewIDs.map((id) => {
        // memoize promises
        if (!(id in viewMemo)) {
          viewMemo[id] = getView(req.access, id)
        }
        return viewMemo[id].then(({ name, view }) => {
          markup.views = markup.views || []
          view.name = name
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
  listQueries,
}

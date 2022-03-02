const { knex } = require('../util/db')
const { APIError, useAPIErrorOptions } = require('../util/api-error')
const { getContext, ERROR_QL_CTX } = require('../util/context')
const { getView, getQueryViews } = require('./views')
const { validateQuery } = require('./engine')
const { updateExecution, queueExecution } = require('./executions')
const { typeToCatMap, CAT_STRING } = require('./type')
const { QL_SCHEMA, MAX_LENGTH_QUERY_DESCRIPTION, MAX_LENGTH_QUERY_NAME } = require('./constants')
const { parseQueryToTree, ParserError } = require('./parser')
const { isInternalUser } = require('./utils')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

/**
 * Returns an array of query metas based on the supplied filters
 * @param {Object} [filters]
 * @param {number} [filters.queryID] Saved query ID
 * @param {-1|number[]} [filters.whitelabelIDs] Array of whitelabel IDs (agency ID)
 * @param {-1|number[]} [filters.customerIDs] Array of customer IDs (agency ID)
 * @param {number} [filters.executionID] Execution ID
 * @param {number} [filters.scheduleID] Schedule ID
 * @param {string} [filters.queryHash] Query hash (unique to the query)
 * @param {string} [filters.columnHash] Column hash (unique to the name/type of the results)
 * @param {boolean} [filters.hideInternal=false] Whether or not to filter out queries
 * using internal fields
 * @param {boolean} [filters.showExecutions=false] Whether or not to append the list of
 * executions to the query object
 * @param {boolean} [filters.showSchedules=false] Whether or not to append the list of
 * schedules to the query object
 * @returns {Promise<Array>}
 */
const getQueryMetas = async ({
  queryID,
  whitelabelIDs,
  customerIDs,
  executionID,
  scheduleID,
  queryHash,
  columnHash,
  hideInternal = false,
  showExecutions = false,
  showSchedules = false,
} = {}) => {
  const { rows } = await knex.raw(`
    SELECT
      q.query_id AS "queryID",
      c.whitelabelid AS "whitelabelID",
      c.customerid AS "customerID",
      c.timezone AS "customerTimezone",
      q.query_hash AS "queryHash",
      q.column_hash AS "columnHash",
      q.name,
      q.description,
      q.query,
      q.view_ids AS "viewIDs",
      q.columns,
      q.is_internal AS "isInternal"
      ${showExecutions ? `, ARRAY(
        SELECT i FROM UNNEST(
          array_agg(
            DISTINCT jsonb_build_object(
              'executionID', e.execution_id,
              'queryHash', e.query_hash,
              'columnHash', e.column_hash,
              'status', e.status,
              'statusTS', e.status_ts,
              'isInternal', e.is_internal
            )
          ) FILTER (WHERE e.execution_id IS NOT NULL)
        ) i ORDER BY i->'executionID' DESC
      ) AS "executions"` : ''}
      ${showSchedules ? `, ARRAY(
        SELECT i - 'scheduleID' FROM UNNEST(
          array_agg(
            DISTINCT jsonb_build_object(
              'scheduleID', sq.schedule_id,
              'cron', s.cron,
              'startDate', sq.start_date,
              'endDate', sq.end_date,
              'isPaused', sq.is_paused
            )
          ) FILTER (WHERE sq.schedule_id IS NOT NULL)
        ) i ORDER BY i->'isPaused', i->'scheduleID'
      ) AS "schedules"` : ''}
    FROM ${QL_SCHEMA}.queries q
    JOIN public.customers c ON c.customerid = q.customer_id
    ${showExecutions || executionID ? `
      LEFT JOIN ${QL_SCHEMA}.executions e ON e.query_id = q.query_id
    ` : ''}
    ${showSchedules || scheduleID ? `
      LEFT JOIN ${QL_SCHEMA}.schedule_queries sq ON sq.query_id = q.query_id
      LEFT JOIN ${QL_SCHEMA}.schedules s ON s.schedule_id = sq.schedule_id
    ` : ''}
    WHERE
      q.is_active
      ${queryID ? 'AND q.query_id = :queryID' : ''}
      ${whitelabelIDs && whitelabelIDs !== -1 ? 'AND c.whitelabelid = ANY(:whitelabelIDs)' : ''}
      ${customerIDs && customerIDs !== -1 ? 'AND c.customerid = ANY(:customerIDs)' : ''}
      ${executionID ? 'AND e.execution_id = :executionID' : ''}
      ${scheduleID ? 'AND sq.schedule_id = :scheduleID' : ''}
      ${queryHash ? 'AND q.query_hash = :queryHash' : ''}
      ${columnHash ? 'AND q.column_hash = :columnHash' : ''}
      ${hideInternal ? 'AND q.is_internal <> TRUE' : ''}
      ${showExecutions && hideInternal ? 'AND e.is_internal <> TRUE' : ''}
    ${showExecutions || showSchedules ? 'GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12' : ''}
  ORDER BY 1 DESC
  `, { queryID, whitelabelIDs, customerIDs, executionID, scheduleID, queryHash, columnHash })
  return rows
}

/**
 * Creates a query
 * @param {number} whitelabelID Whitelabel ID
 * @param {number} customerID Customer ID (agency ID)
 * @param {string} queryHash Query hash (unique to the query)
 * @param {string} columnHash Column hash (unique to the name/type of the results)
 * @param {string} name Query name
 * @param {Object} query Query object
 * @param {string[]} viewsIDs List of the query views' IDs
 * @param {[string, number][]} columns List of the query columns formatted as [name, pgTypeOID]
 * @param {boolean} isInternal Whether or not the query accesses views restricted to internal users
 * @param {string} [description] Query description
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 * @returns {Promise<number>} The query ID
 */
const createQuery = async (
  whitelabelID,
  customerID,
  queryHash,
  columnHash,
  name,
  query,
  viewIDs,
  columns,
  isInternal,
  description,
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

  const safeName = name.slice(0, MAX_LENGTH_QUERY_NAME)
  if (description) {
    cols.push('description')
    values.push(description.slice(0, MAX_LENGTH_QUERY_DESCRIPTION))
  }

  // rewrite name when duplicate
  // populate query_id manually via nextval so that it can be accessed by currval
  const expressionCols = ['query_id', 'name']
  const expressions = [
    `nextval(pg_get_serial_sequence('${QL_SCHEMA}.queries', 'query_id'))`,
    `
      CASE WHEN EXISTS (
        SELECT query_id FROM ${QL_SCHEMA}.queries
        WHERE
          customer_id = ?
          AND name = ?
      ) THEN ? || ' - ' || currval(pg_get_serial_sequence('${QL_SCHEMA}.queries', 'query_id'))
      ELSE ? END
    `,
  ]
  const expressionValues = [customerID, safeName, safeName, safeName]

  const { rows: [{ queryID }] } = await knexClient.raw(`
    WITH access AS (
      SELECT customerid FROM public.customers
      WHERE
        whitelabelid = ?
        AND customerid = ?
    )
    INSERT INTO ${QL_SCHEMA}.queries
      (${[...cols, ...expressionCols].join(', ')})
      SELECT ${cols.map(() => '?').concat(expressions).join(', ')}
      WHERE EXISTS (SELECT * FROM access)
    RETURNING query_id AS "queryID"
  `, [...values, ...expressionValues])

  return queryID
}

/**
 * Updates a query based on its id
 * @param {number} queryID Query ID
 * @param {Object} updates
 * @param {string} [updates.queryHash] Query hash (unique to the query)
 * @param {string} [updates.columnHash] Column hash (unique to the name/type of the results)
 * @param {string} [updates.name] Query name
 * @param {Object} [updates.query] Query object
 * @param {string[]} [updates.viewsIDs] List of the query views' IDs
 * @param {[string, number][]} [updates.columns] List of the query columns
 * formatted as [name, pgTypeOID]
 * @param {boolean} [updates.isInternal] Whether or not the query accesses views restricted to
 * internal users
 * @param {string|null} [updates.description] Query description or null to remove
 * @param {boolean} [updates.isActive] Active status
 * @param {Knex} [knexClient=knex] Knex client to use to run the SQL query. Defaults to the
 * global client
 */
const updateQuery = async (
  queryID,
  { queryHash, columnHash, name, query, viewIDs, columns, isInternal, description, isActive },
  knexClient = knex,
) => {
  const cols = []
  const values = []
  const expressions = []
  const expressionValues = []
  if (queryHash) {
    cols.push('query_hash')
    values.push(queryHash)
  }
  if (columnHash) {
    cols.push('column_hash')
    values.push(columnHash)
  }
  if (name) {
    const safeName = name.slice(0, MAX_LENGTH_QUERY_NAME)
    expressions.push(`
      name = CASE WHEN EXISTS (
        SELECT query_id FROM ${QL_SCHEMA}.queries
        WHERE
          customer_id = q.customer_id
          AND query_id <> q.query_id
          AND name = ?
      ) THEN ? || ' - ' || query_id ELSE ? END
    `)
    expressionValues.push(safeName, safeName, safeName)
  }
  if (query) {
    cols.push('query')
    values.push(JSON.stringify(query))
  }
  if (viewIDs) {
    cols.push('view_ids')
    values.push(JSON.stringify(viewIDs))
  }
  if (columns) {
    cols.push('columns')
    values.push(JSON.stringify(columns))
  }
  if (isInternal !== undefined) {
    cols.push('is_internal')
    values.push(isInternal)
  }
  if (description !== undefined) {
    cols.push('description')
    values.push(description || null)
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
    UPDATE ${QL_SCHEMA}.queries q
    SET ${cols.map(col => `${col} = ?`).concat(expressions).join(', ')}
    WHERE query_id = ?
  `, [...values, ...expressionValues, queryID])
}

const postQuery = async (req, res, next) => {
  try {
    const { name, description } = req.body
    const {
      access: { whitelabel, customers },
      ql: { views, tree },
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = req
    const { executionID, queryID: executionQueryID, isOrphaned } = req.ql.execution || {}
    if (typeof name !== 'string') {
      throw apiError(`Query name must be a string of maximum length ${MAX_LENGTH_QUERY_NAME}`)
    }
    const safeName = name.trim()
    if (!safeName || safeName.length > MAX_LENGTH_QUERY_NAME) {
      throw apiError(`Query name must be a string of maximum length ${MAX_LENGTH_QUERY_NAME}`)
    }
    let safeDescription
    if (description !== undefined) {
      if (typeof description !== 'string') {
        throw apiError(`Query description must be a string of\
maximum length ${MAX_LENGTH_QUERY_DESCRIPTION}`)
      }
      safeDescription = description.trim()
      if (!safeDescription || safeDescription.length > MAX_LENGTH_QUERY_DESCRIPTION) {
        throw apiError(`Query description must be a string of\
maximum length ${MAX_LENGTH_QUERY_DESCRIPTION}`)
      }
    }
    const query = tree.toQL({ keepParamRefs: !tree.hasParameterValues() })
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(views).some(v => v.isInternal)

    // create query + update execution (as applicable) in transaction
    const queryID = await knex.transaction(async (trx) => {
      const queryID = await createQuery(
        whitelabel[0],
        customers[0],
        mlQueryHash,
        mlQueryColumnHash,
        safeName,
        query,
        Object.keys(views),
        mlQueryColumns,
        isInternal,
        safeDescription,
        trx,
      )
      getContext(req, ERROR_QL_CTX).queryID = queryID
      // if execution supplied, attach it to the created query if not already attached to a query
      if (executionID && !executionQueryID && !isOrphaned) {
        await updateExecution(executionID, { queryID }, { knexClient: trx })
      }
      return queryID
    })

    res.json({ queryID })
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to save the query', 500))
  }
}

const putQuery = async (req, res, next) => {
  try {
    const { queryID } = req.ql.query
    const { name, description } = req.body
    const { views, tree } = req.ql
    const { mlQueryHash, mlQueryColumnHash, mlQueryColumns } = req
    if (!name) {
      throw apiError('Query name cannot be empty')
    }
    if (typeof name !== 'string') {
      throw apiError(`Query name must be a string of maximum length ${MAX_LENGTH_QUERY_NAME}`)
    }
    const safeName = name.trim()
    if (!safeName || safeName.length > MAX_LENGTH_QUERY_NAME) {
      throw apiError(`Query name must be a string of maximum length ${MAX_LENGTH_QUERY_NAME}`)
    }
    let safeDescription = null
    if (description !== undefined && description !== null) {
      if (typeof description !== 'string') {
        throw apiError(`Query description must be a string of\
maximum length ${MAX_LENGTH_QUERY_DESCRIPTION}`)
      }
      safeDescription = description.trim()
      if (!safeDescription || safeDescription.length > MAX_LENGTH_QUERY_DESCRIPTION) {
        throw apiError(`Query description must be a string of\
maximum length ${MAX_LENGTH_QUERY_DESCRIPTION}`)
      }
    }
    const query = tree.toQL({ keepParamRefs: !tree.hasParameterValues() })
    // determine whether or not query uses internal-only views
    const isInternal = Object.values(views).some(v => v.isInternal)
    await updateQuery(queryID, {
      name: safeName,
      queryHash: mlQueryHash,
      columnHash: mlQueryColumnHash,
      query,
      viewIDs: Object.keys(views),
      columns: mlQueryColumns,
      isInternal,
      description: safeDescription,
    })
    res.json({ queryID })
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to update the query', 500))
  }
}

const deleteQuery = async (req, res, next) => {
  try {
    const { queryID } = req.ql.query
    await updateQuery(queryID, { isActive: false })
    res.json({ queryID })
  } catch (err) {
    next(getSetAPIError(err, 'Failed to delete the query', 500))
  }
}

/**
 * Queues an execution for a query given its ID
 * @param {number} queryID Query ID
 * @param {number} [scheduleJobID] The ID of the schedule job which triggered the execution, if any
 * @returns {number} Execution ID
 */
const queueQueryExecution = async (queryID, scheduleJobID, engine = 'pg') => {
  try {
    const [queryMeta] = await getQueryMetas({ queryID })
    if (!queryMeta) {
      throw apiError('Invalid query ID')
    }
    const { whitelabelID, customerID, query, isInternal } = queryMeta
    const access = {
      whitelabel: [whitelabelID],
      customers: [customerID],
      prefix: isInternal ? 'internal' : 'customers',
    }

    const tree = parseQueryToTree(query, { type: 'ql', paramsMustHaveValues: true })

    // get query views
    const views = await getQueryViews(access, tree.viewColumns)

    const {
      mlQueryHash,
      mlQueryColumnHash,
      mlQueryColumns,
    } = await validateQuery(whitelabelID, customerID, views, tree, engine)

    const executionID = await queueExecution(
      whitelabelID,
      customerID,
      mlQueryHash,
      mlQueryColumnHash,
      query,
      views,
      mlQueryColumns,
      { queryID, scheduleJobID },
    )

    return executionID
  } catch (err) {
    if (err instanceof ParserError) {
      throw apiError(err.message, 400)
    }
    throw err
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
      showSchedules: true,
    })
    if (!query) {
      throw apiError('Invalid query ID', 404)
    }
    // attach to req.ql
    req.ql.query = query
    getContext(req, ERROR_QL_CTX).queryID = queryID
    // set customer to that of the query
    req.access = {
      ...access,
      whitelabel: [query.whitelabelID],
      customers: [query.customerID],
    }
    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to load the query', 500))
  }
}

const respondWithQuery = async (req, res, next) => {
  try {
    const { query } = req.ql
    const { viewIDs, columns } = query

    // convert columns from array to object
    query.columns = columns.map(([name, pgType]) => ({
      name,
      category: typeToCatMap.get(pgType) || CAT_STRING,
    }))

    // populate views (instead of viewIDs)
    delete query.viewIDs
    query.views = query.views || []
    await Promise.all(viewIDs.map(id => getView(req.access, id).then(({ name, view }) => {
      // query.views = query.views || []
      view.name = name
      query.views.push(view)
    }).catch((err) => {
      // edge case when view has been unsubscribed or is no longer available
      // soft fail
      query.views.push({
        id,
        error: (err instanceof APIError && err.message) || 'View could not be retrieved',
      })
    })))
    res.json(query)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the query', 500))
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
      const { viewIDs, columns } = q

      // convert columns from array to object
      q.columns = columns.map(([name, pgType]) => ({
        name,
        category: typeToCatMap.get(pgType) || CAT_STRING,
      }))

      // populate views
      delete q.viewIDs
      q.views = q.views || []
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
          q.views.push(view)
        })
      }))
      return acc
    }, []))
    res.json(queries)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve the queries', 500))
  }
}

module.exports = {
  createQuery,
  postQuery,
  putQuery,
  deleteQuery,
  queueQueryExecution,
  loadQuery,
  respondWithQuery,
  listQueries,
}

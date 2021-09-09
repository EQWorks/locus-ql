/* eslint-disable valid-typeof */
/* eslint-disable func-names */
/* eslint-disable no-nested-ternary */
const { createHash } = require('crypto')

const { knex, mlPool, knexBuilderToRaw, fdwConnectByName } = require('../util/db')
const { Expression } = require('./expressions')
const { insertGeo } = require('./geo')
const { useAPIErrorOptions } = require('../util/api-error')
const { knexWithCache, queryWithCache, cacheTypes } = require('./cache')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

// const TYPE_STRING = 'string'

const JOIN_TYPES = ['left', 'right', 'inner']

/**
 * Converts all non-array objects to sorted arrays of key/value pairs
 * Can be used to obtain a normalized value of an object
 * @param {Object} object Object
 * @returns {any} Array or non-object value
 */
const sortObject = (object) => {
  // return non-object as is
  if (typeof object !== 'object') {
    return object
  }
  // return null as undefined
  if (object === null) {
    return
  }
  // sort array elements and filter out undefined elements
  // return undefined if empty array
  if (Array.isArray(object)) {
    const objectvalue = object.map(i => sortObject(i)).filter(i => i !== undefined)
    return objectvalue.length ? objectvalue : undefined
  }
  // return object as sorted array of key/value pairs
  // remove undefined keys (e.g. undefined, empty array, null, array with only null entries...)
  const objectValue = Object.entries(object).reduce((value, [k, v]) => {
    const sorted = sortObject(v)
    if (sorted !== undefined) {
      value.push([k, sorted])
    }
    return value
  }, [])
  // return undefined if empty array
  if (!objectValue.length) {
    return
  }
  // sort so results are consistent across calls
  return objectValue.sort(([kA], [kB]) => {
    if (kA < kB) {
      return -1
    }
    if (kA > kB) {
      return 1
    }
    return 0
  })
}

/**
 * Computes a hash for an object based on its JSON value
 * The hash can be used to version the object
 * @param {Object} object Object
 * @returns {string} Hash
 */
const getObjectHash = object => createHash('sha256')
  .update(JSON.stringify(sortObject(object)))
  .digest('base64')

// return viewID if it exists in views
const getView = (views, viewID) => {
  if (!views[viewID]) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  return viewID

  // reserve for complex viewID, when viewID can be sub query object
  // if (typeof viewID === TYPE_STRING) {
  //   const view = views[viewID]
  //   if (view) {
  //     return view
  //   }
  //   throw apiError(`Invalid view: ${viewID}`, 403)
  // } else {
  //   throw apiError(`Invalid view: ${viewID}`, 403)
  // }
}

// TODO: think through multiple DB case
// should db be in views? Should first view determine db etc
const select = (
  views,
  viewColumns,
  {
    distinct,
    columns,
    from,
    joins = [],
    where = [],
    having = [],
    groupBy,
    orderBy,
    limit,
    // db = 'place',
  },
) => {
  const exp = new Expression(viewColumns)

  // attach all views as CTE's
  const knexQuery = Object.entries(views)
    // '__source' > '__geo' > rest
    .sort(([idA], [idB]) => {
      const idValueA = idA.startsWith('__source') ? 2 : idA.startsWith('__geo') ? 1 : 0
      const idValueB = idB.startsWith('__source') ? 2 : idB.startsWith('__geo') ? 1 : 0
      return idValueB - idValueA
    })
    .reduce((knex, [id, view]) => knex.with(id, view), knex)

  // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
  knexQuery
    .column(columns.map(exp.parseExpression.bind(exp)))
    .from(getView(views, from))

  // Where
  exp.parseConditions(
    where,
    knexQuery.where.bind(knexQuery),
    knexQuery.whereRaw.bind(knexQuery),
  )

  // Having
  exp.parseConditions(
    having,
    knexQuery.having.bind(knexQuery),
    knexQuery.havingRaw.bind(knexQuery),
  )

  // Distinct Flag
  if (distinct) {
    knexQuery.distinct()
  }

  // Group By
  if (groupBy && groupBy.length > 0) {
    knexQuery.groupByRaw(groupBy.map(exp.parseExpression.bind(exp)).join(', '))
  }

  // Order By
  if (orderBy && orderBy.length > 0) {
    knexQuery.orderByRaw(orderBy.map(exp.parseExpression.bind(exp)).join(', '))
  }

  // JOINs
  joins.forEach((join) => {
    if (!JOIN_TYPES.includes(join.joinType)) {
      throw apiError(`Invalid join type: ${join.joinType}`, 403)
    }
    knexQuery[`${join.joinType}Join`](getView(views, join.view), function () {
      exp.parseConditions(
        join.on,
        this.on.bind(this),
      )
    })
  })

  // LIMIT
  if (limit || limit === 0) {
    if (Number.isInteger(limit) && limit >= 0) {
      knexQuery.limit(limit)
    } else {
      throw apiError(`Invalid limit: ${limit}`, 403)
    }
  }

  return knexQuery
}

// parses query to knex object
const getKnexQuery = (views, viewColumns, query) => {
  const { type } = query
  if (type === 'select') {
    return select(views, viewColumns, query)
  }
}

/**
 * Establishes connections with foreign databases
 * @param {PG.PoolClient} pgConnection PG connection to use to connect to the foreign db's
 * @param {Object.<string, string[]>} fdwConnections Map of view ID's and array of connection names
 * @param {number} [timeout] Connection timeout in seconds
 * @returns {Promise<undefined>}
 */
const establishFdwConnections = (pgConnection, fdwConnections, timeout) => {
  // remove duplicates
  const uniqueConnections = [...(new Set(Object.values(fdwConnections).flat()))]
  return Promise.all(uniqueConnections.map(connectionName => fdwConnectByName(
    pgConnection,
    { connectionName, timeout },
  )))
}

// runs query with cache
const executeQuery = (views, viewColumns, query, { pgConnection, maxAge }) => {
  const knexQuery = getKnexQuery(views, viewColumns, query)
  if (pgConnection) {
    knexQuery.connection(pgConnection)
  }
  return knexWithCache(
    knexQuery,
    { ttl: 1800, maxAge, type: cacheTypes.S3 }, // 30 minutes (subject to maxAge)
  )
}

/**
 * Parses and validates query by running it with a limit of 0
 * @param {Object.<string, Knex.QueryBuilder|Knex.Raw>} views Map of view ID's and knex view objects
 * @param {Object.<string, Object>} viewColumns Map of view ID's and column objects
 * @param {Object.<string, string[]>} fdwConnections Map of view ID's and array of connection names
 * @param {Object} query Query object
 * @param {Object} access Access object
 * @param {number[]|-1} access.whitelabel
 * @param {number[]|-1} access.customers
 * @param {string} access.prefix
 * @returns {Promise<Object>} Query and result column hashes along with result schema
 */
const validateQuery = async (views, viewColumns, fdwConnections, query, access) => {
  // insert geo views & joins
  const [
    queryWithGeo,
    viewsWithGeo,
    fdwConnectionsWithGeo,
  ] = insertGeo(access, views, viewColumns, fdwConnections, query)

  // if no error then query was parsed successfully
  const knexQuery = getKnexQuery(viewsWithGeo, viewColumns, queryWithGeo)

  // check out PG connection to use for fdw + query (must be same)
  const pgConnection = await mlPool.connect()
  let fields
  try {
    // establish fdw connections
    await establishFdwConnections(pgConnection, fdwConnectionsWithGeo)

    // set connection on ml query
    knexQuery.connection(pgConnection)

    // run the query with limit 0
    knexQuery.limit(0)
    const { sql, bindings } = knexQuery.toSQL()
    fields = await queryWithCache(
      [sql, bindings, 'fields'],
      () => knexBuilderToRaw(knexQuery).then(({ fields }) => fields), // only cache fields
      { ttl: 86400, type: cacheTypes.REDIS, rows: false }, // 1 day
    )
  } finally {
    pgConnection.release()
  }

  const columns = fields.map(({ name, dataTypeID }) => [name, dataTypeID])
  return {
    mlQueryHash: getObjectHash(query),
    mlQueryColumnHash: getObjectHash(columns),
    mlQueryColumns: columns,
  }
}

// throws an error if the query cannot be parsed
// attaches queryHash, columnHash and columns to req
const validateQueryMW = (onlyUseBodyQuery = false) => async (req, _, next) => {
  try {
    // if a saved query or execution have been attached to req, use it
    // else use req.body
    const loadedQuery = !onlyUseBodyQuery && (req.mlQuery || req.mlExecution)
    const { query } = loadedQuery || req.body
    const { mlViews, mlViewColumns, mlViewFdwConnections, access } = req

    // get query and column hashes + results schema and attach to req
    const values = await validateQuery(mlViews, mlViewColumns, mlViewFdwConnections, query, access)
    Object.assign(req, values)
    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to parse the query', 500))
  }
}

module.exports = {
  getKnexQuery,
  executeQuery,
  validateQuery,
  validateQueryMW,
  establishFdwConnections,
}


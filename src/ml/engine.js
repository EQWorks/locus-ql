/* eslint-disable valid-typeof */
/* eslint-disable func-names */
/* eslint-disable no-nested-ternary */
const { createHash } = require('crypto')

const { knex, knexBuilderToRaw, fdwConnectByName } = require('../util/db')
const { Expression } = require('./expressions')
const { insertGeo } = require('./geo')
const { apiError, APIError } = require('../util/api-error')
const { knexWithCache, queryWithCache, cacheTypes } = require('./cache')


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

const getView = (views, viewID) => {
  if (!views[viewID]) {
    throw apiError(`Invalid view: ${viewID}`, 403)
  }
  return views[viewID]

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
  const knexQuery = knex
    // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
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
const getKnexQuery = (access, views, viewColumns, query) => {
  const [queryWithGeo, viewsWithGeo] = insertGeo(access, views, viewColumns, query)
  const { type } = query
  if (type === 'select') {
    return select(viewsWithGeo, viewColumns, queryWithGeo)
  }
}

/**
 * Establishes connections with foreign databases
 * @param {Object.<string, string[]>} fdwConnections Map of view ID's and array of connection names
 * @returns {Promise<undefined>}
 */
const establishFdwConnections = (fdwConnections) => {
  // remove duplicates
  const uniqueConnections = [...(new Set(Object.values(fdwConnections).flat()))]
  return Promise.all(uniqueConnections.map(conn => fdwConnectByName(conn)))
}

// runs query with cache
const executeQuery = (access, views, viewColumns, query, maxAge) => {
  const knexQuery = getKnexQuery(access, views, viewColumns, query)
  return knexWithCache(
    knexQuery,
    { ttl: 1800, maxAge, type: cacheTypes.S3 }, // 30 minutes (subject to maxAge)
  )
}

// throws an error if the query cannot be parsed
// attaches queryHash, columnHash and columns to req
const validateQuery = (onlyUseBodyQuery = false) => async (req, _, next) => {
  try {
    // if a saved query or execution have been attached to req, use it
    // else use req.body
    const loadedQuery = !onlyUseBodyQuery && (req.mlQuery || req.mlExecution)
    const { query } = loadedQuery || req.body
    const { mlViews, mlViewColumns, mlViewFdwConnections, access } = req

    // if no error then query was parsed successfully
    const knexQuery = getKnexQuery(access, mlViews, mlViewColumns, query)

    // establish fdw connections
    await establishFdwConnections(mlViewFdwConnections)

    // run the query with limit 0
    knexQuery.limit(0)
    const { sql, bindings } = knexQuery.toSQL()
    const fields = await queryWithCache(
      [sql, bindings],
      () => knexBuilderToRaw(knexQuery).then(({ fields }) => fields), // only cache fields
      { ttl: 86400, type: cacheTypes.REDIS, rows: false }, // 1 day
    )
    const columns = fields.map(({ name, dataTypeID }) => [name, dataTypeID])
    req.mlQueryHash = getObjectHash(query)
    req.mlQueryColumnHash = getObjectHash(columns)
    req.mlQueryColumns = columns
    next()
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to parse the query'))
  }
}

module.exports = {
  getKnexQuery,
  executeQuery,
  validateQuery,
  establishFdwConnections,
}


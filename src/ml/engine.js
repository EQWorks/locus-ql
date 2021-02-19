/* eslint-disable valid-typeof */
/* eslint-disable func-names */
/* eslint-disable no-nested-ternary */
const { createHash } = require('crypto')

const { knex, mapKnex, knexBuilderToRaw } = require('../util/db')
const { Expression } = require('./expressions')
const apiError = require('../util/api-error')
const { knexWithCache, cacheTypes } = require('./cache')
const { typeToCatMap, CAT_STRING } = require('./type')


// const TYPE_STRING = 'string'

const JOIN_TYPES = ['left', 'right', 'inner']

/**
 * Computes a hash for an object based on its JSON value
 * The hash can be used to version the object
 * @param {Object} object Object
 * @returns {string} Hash
 */
const getObjectHash = object => createHash('sha256').update(JSON.stringify(object)).digest('base64')

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
  let knexDB = knex
  // only layers need db to be map(?)
  // 'layer_958_0' - `layer_${layer_id}_${categoryKey}`
  if (from.startsWith('layer')) {
  // if (db === 'map') {
    knexDB = mapKnex
  }
  let knexQuery = knexDB
    // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
    .column(columns.map(exp.parseExpression.bind(exp)))
    .from(getView(views, from))
    .where(function () {
      where.forEach((whereStatement) => {
        if (Array.isArray(whereStatement)) {
          const [argA, operator, argB] = whereStatement
          this.where(
            typeof argA === 'object' ? exp.parseExpression(argA) : argA,
            operator,
            argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
          )
        } else if (whereStatement) {
          this.whereRaw(exp.parseExpression(whereStatement))
        }
      })
    })

  // Having
  if (having.length > 0) {
    having.forEach((havingStatement) => {
      if (Array.isArray(havingStatement)) {
        const [argA, operator, argB] = havingStatement
        knexQuery.having(
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB,
        )
      } else if (havingStatement) {
        knexQuery.havingRaw(exp.parseExpression(havingStatement))
      }
    })
  }

  // Distinct Flag
  if (distinct) {
    knexQuery = knexQuery.distinct()
  }

  // Group By
  if (groupBy && groupBy.length > 0) {
    knexQuery = knexQuery.groupByRaw(groupBy.map(exp.parseExpression.bind(exp)).join(', '))
  }

  // Order By
  if (orderBy && orderBy.length > 0) {
    knexQuery = knexQuery.orderByRaw(orderBy.map(exp.parseExpression.bind(exp)).join(', '))
  }

  // JOINs
  joins.forEach((join) => {
    if (!JOIN_TYPES.includes(join.joinType)) {
      throw apiError(`Invalid join type: ${join.joinType}`, 403)
    }
    const joinFuncName = `${join.joinType}Join`


    knexQuery[joinFuncName](getView(views, join.view), function () {
      const conditions = join.on

      // handle easy array form conditions
      // where condition is in format of: [argA, operator, argB]
      if (Array.isArray(conditions)) {
        conditions.forEach(([argA, operator, argB]) => this.on(
          // to avoid adding ' ' to argument
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
        )) // validate conditions filters?
      } else {
        // handle complex conditions
        this.on(exp.parseExpression(conditions))
      }
    })
  })

  // LIMIT
  if (limit || limit === 0) {
    if (Number.isInteger(limit) && limit >= 0) {
      knexQuery = knexQuery.limit(limit)
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

// runs query with cache
const executeQuery = (views, viewColumns, query, maxAge) => {
  const knexQuery = getKnexQuery(views, viewColumns, query)
  return knexWithCache(
    knexQuery,
    { ttl: 1800, maxAge, type: cacheTypes.S3 }, // 30 minutes (subject to maxAge)
  )
}

// throws an error if the query cannot be parsed
// attaches queryHash, columnHash and columns to req
const validateQuery = async (req, _, next) => {
  try {
    // if a saved query or execution have been attached to req, use it
    // else use req.body
    const reqQuery = req.mlQuery || req.mlExecution
    const { query } = reqQuery ? reqQuery.markup : req.body
    const { mlViews, mlViewColumns } = req

    // if no error then query was parsed successfully
    const knexQuery = getKnexQuery(mlViews, mlViewColumns, query)

    // run the query with limit 0
    knexQuery.limit(0)
    const { fields } = await knexWithCache(
      knexBuilderToRaw(knexQuery),
      { ttl: 86400, type: cacheTypes.REDIS, rows: false }, // 1 day
    )
    const columns = fields.map(({ name, dataTypeID }) => ([
      name,
      typeToCatMap.get(dataTypeID) || CAT_STRING,
    ]))
    req.mlQueryHash = getObjectHash(query)
    req.mlQueryColumnHash = getObjectHash(columns)
    req.mlQueryColumns = columns
    next()
  } catch (err) {
    next(err)
  }
}

module.exports = {
  getKnexQuery,
  executeQuery,
  validateQuery,
}


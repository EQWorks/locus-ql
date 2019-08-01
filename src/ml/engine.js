/* eslint-disable valid-typeof */
/* eslint-disable func-names */

const { knex } = require('../util/db')
const { Expression } = require('./expressions')
const apiError = require('../util/api-error')


// const TYPE_STRING = 'string'

const JOIN_TYPES = ['left', 'right', 'inner']

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

const select = async (
  views,
  viewColumns,
  { distinct, columns, from, joins = [], where = [], groupBy, limit },
) => {
  const exp = new Expression(viewColumns)

  let knexQuery = knex
    // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
    .columns(columns.map(exp.parseExpression.bind(exp)))
    .from(getView(views, from))
    .where(function () {
      // handle simple array form conditions
      // where condition is in format of: [argA, operator, argB]
      if (Array.isArray(where)) {
        where.forEach(([argA, operator, argB]) => this.where(
          // to avoid adding ' ' to argument
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          typeof argB === 'object' ? exp.parseExpression(argB) : argB,
        )) // validate where operator and columns?
        return
      }

      // handle complex conditions
      this.whereRaw(exp.parseExpression(where))
    })

  // Distinct Flag
  if (distinct) {
    knexQuery = knexQuery.distinct()
  }

  // Group By
  if (groupBy && groupBy.length > 0) {
    knexQuery = knexQuery.groupByRaw(groupBy.map(exp.parseExpression.bind(exp)).join(', '))
  }

  // JOINs
  joins.forEach((join) => {
    if (!JOIN_TYPES.includes(join.joinType)) {
      throw apiError(`Invalid join type: ${join.joinType}`, 403)
    }
    const joinFuncName = `${join.joinType}Join`

    console.log('joinFuncName:', joinFuncName)

    knexQuery[joinFuncName](getView(views, join.view), function () {
      const conditions = join.on
      // handle easy array form conditions
      // where condition is in format of: [argA, operator, argB]
      if (Array.isArray(conditions)) {
        conditions.forEach(([argA, operator, argB]) => this.on(
          // to avoid adding ' ' to argument
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          typeof argB === 'object' ? exp.parseExpression(argB) : argB,
        )) // validate conditions filters?
      }

      // TODO: not supporting directly passing complex expression right now,
      // which means no nested logic operators like:
      // JOIN xxx ON name = 'abc' and (age = '13' or birth is null)
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

module.exports.execute = async (views, viewColumns, query) => {
  const { type } = query

  if (type === 'select') {
    return select(views, viewColumns, query)
  }
}


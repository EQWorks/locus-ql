/* eslint-disable valid-typeof */
/* eslint-disable func-names */

const { knex } = require('../util/db')
const { parseExpression } = require('./expressions')
const apiError = require('../util/api-error')


// const TYPE_STRING = 'string'

const JOIN_TYPES = ['left', 'right', 'inner']

const parseView = (views, viewID) => {
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

const select = async (views, { columns, from, joins = [], where = [], groupBy }) => {
  console.log('begining selection')

  let knexQuery = knex
    .columns(columns.map(parseExpression))
    .from(parseView(views, from))
    .where(function () {
      // handle simple array form conditions
      // where condition is in format of: [argA, operator, argB]
      if (Array.isArray(where)) {
        where.forEach(([argA, operator, argB]) => this.where(
          // to avoid adding ' ' to argument
          typeof argA === 'object' ? parseExpression(argA) : argA,
          operator,
          typeof argB === 'object' ? parseExpression(argB) : argB,
        )) // validate where operator and columns?
        return
      }

      // handle complex conditions
      this.whereRaw(parseExpression(where))
    })

  // Group By
  if (groupBy && groupBy.length > 0) {
    knexQuery = knexQuery.groupByRaw(groupBy.map(parseExpression).join(', '))
  }

  // JOINs
  joins.forEach((join) => {
    if (!JOIN_TYPES.includes(join.joinType)) {
      throw apiError(`Invalid join type: ${join.joinType}`, 403)
    }
    const joinFuncName = `${join.joinType}Join`

    console.log('joinFuncName:', joinFuncName)

    knexQuery[joinFuncName](parseView(views, join.view), function () {
      const conditions = join.on
      // handle easy array form conditions
      // where condition is in format of: [argA, operator, argB]
      if (Array.isArray(conditions)) {
        conditions.forEach(([argA, operator, argB]) => this.on(
          // to avoid adding ' ' to argument
          typeof argA === 'object' ? parseExpression(argA) : argA,
          operator,
          typeof argB === 'object' ? parseExpression(argB) : argB,
        )) // validate conditions filters?
      }

      // TODO: not supporting directly passing complex expression right now,
      // which means no nested logic operators like:
      // JOIN xxx ON name = 'abc' and (age = '13' or birth is null)
    })
  })
  return knexQuery
}

module.exports.execute = async (views, query) => {
  const { type } = query

  if (type === 'select') {
    return select(views, query)
  }
}


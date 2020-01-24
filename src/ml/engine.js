/* eslint-disable valid-typeof */
/* eslint-disable func-names */
/* eslint-disable no-nested-ternary */

const { knex, mapKnex } = require('../util/db')
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

// TODO: think through multiple DB case
// should db be in views? Should first view determine db etc
const select = async (
  views,
  viewColumns,
  { distinct, columns, from, joins = [], where = [], groupBy, orderBy, limit, db = 'place' },
) => {
  const exp = new Expression(viewColumns)
  let knexDB = knex
  if (db === 'map') {
    knexDB = mapKnex
  }

  const orWhereArr = []
  const andWhereArr = []

  if (where.length > 1) {
    where.forEach((whereStatement) => {
      const { values } = whereStatement
      if (typeof whereStatement === 'object' && whereStatement.type === 'or') {
        orWhereArr.push(values)
      } else if (typeof whereStatement === 'object' && whereStatement.type === 'and') {
        andWhereArr.push(values)
      }
    })
  }

  let knexQuery = knexDB
    // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
    .column(columns.map(exp.parseExpression.bind(exp)))
    .from(getView(views, from))
    .where(function () {
      if (Array.isArray(where[0])) {
        const [argA, operator, argB] = where[0]
        this.where(
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
        )
      } else if (where[0]) {
        this.whereRaw(exp.parseExpression(where[0]))
      }
    })
    .andWhere(function () {
      if (andWhereArr.length > 0) {
        andWhereArr.forEach((andWhere) => {
          if (Array.isArray(andWhere[0])) {
            const [argA, operator, argB] = andWhere
            this.where(
              typeof argA === 'object' ? exp.parseExpression(argA) : argA,
              operator,
              argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
            )
          } else if (andWhere[0]) {
            this.whereRaw(exp.parseExpression(andWhere[0]))
          }
        })
      }
    })
    .orWhere(function () {
      // if using 'this.where' (instead of 'this.orWhere'):
      // maping through 'orWhere' array within orWhere
      // generates chaining 'AND' statements within 'orWhere'
      if (orWhereArr.length > 0) {
        orWhereArr.forEach((orWhere) => {
          if (Array.isArray(orWhere[0])) {
            const [argA, operator, argB] = orWhere
            this.orWhere(
              typeof argA === 'object' ? exp.parseExpression(argA) : argA,
              operator,
              argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
            )
          } else if (orWhereArr[0]) {
            this.orWhere(exp.parseExpression(orWhere[0]))
          }
        })
      }
    })

    // Handle additional where statements
    // TODO: some way to distinguish between "orWhere" and "andWhere"
    // .andWhere(function () {
    //   if (where.length > 1) {
    //     where.slice(1).forEach((whereStatement) => {
    //       if (Array.isArray(whereStatement)) {
    //         const [argA, operator, argB] = whereStatement
    //         this.where(
    //           typeof argA === 'object' ? exp.parseExpression(argA) : argA,
    //           operator,
    //           argB === null ? argB : (typeof argB === 'object' ?
    //           exp.parseExpression(argB) : argB),
    //         )
    //       } else if (where[0]) {
    //         this.whereRaw(exp.parseExpression(whereStatement))
    //       } else {
    //         return null
    //       }
    //     })
    //   }
    // })

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

module.exports.execute = async (views, viewColumns, query) => {
  const { type } = query

  if (type === 'select') {
    return select(views, viewColumns, query)
  }
}


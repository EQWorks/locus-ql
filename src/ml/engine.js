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

  const whereArr = where[0]
  const orWhereArr = []
  const andWhereArr = []

  if (where.length > 1) {
    where.forEach((whereStatement) => {
      const { values } = whereStatement[0]
      if (typeof whereStatement === 'object' && whereStatement[0].type === 'or') {
        if (whereStatement.length > 1) {
          orWhereArr.push(whereStatement)
        } else {
          orWhereArr.push(values)
        }
      } else if (typeof whereStatement === 'object' && whereStatement[0].type === 'and') {
        if (whereStatement.length > 1) {
          andWhereArr.push(whereStatement)
        } else {
          andWhereArr.push(values)
        }
      }
    })
  }

  let knexQuery = knexDB
    // use bind() here to prevent exp instance from getting lost, same for other bind() usage below
    .column(columns.map(exp.parseExpression.bind(exp)))
    .from(getView(views, from))
    .where(function () {
      let orArr
      let andArr
      let argA
      let operator
      let argB

      whereArr.forEach((where) => {
        if (Array.isArray(where)) {
          const [argumentA, op, argumentB] = where
          argA = argumentA
          operator = op
          argB = argumentB
        } else if (typeof where === 'object' && where.type === 'or') {
          orArr = where.values
        } else if (typeof where === 'object' && where.type === 'and') {
          andArr = where.values
        }
      })

      if (orArr && orArr.length > 0 && !andArr) {
        this.where(
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
        ).orWhere(
          typeof orArr[0] === 'object' ? exp.parseExpression(orArr[0]) : orArr[0],
          orArr[1],
          orArr[2] === null ? orArr[2] :
            (typeof orArr[2] === 'object' ? exp.parseExpression(orArr[2]) : orArr[2]),
        )
      } else if (andArr && andArr.length > 0 && !orArr) {
        this.where(
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
        ).andWhere(
          typeof andArr[0] === 'object' ? exp.parseExpression(andArr[0]) : andArr[0],
          andArr[1],
          andArr[2] === null ? andArr[2] :
            (typeof andArr[2] === 'object' ? exp.parseExpression(andArr[2]) : andArr[2]),
        )
      } else if (typeof whereArr[0] === 'object' && !Array.isArray(whereArr[0])) {
        this.whereRaw(exp.parseExpression(whereArr[0]))
      } else {
        this.where(
          typeof argA === 'object' ? exp.parseExpression(argA) : argA,
          operator,
          argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
        )
      }
    })
    .andWhere((builder) => {
      if (andWhereArr.length > 0) {
        andWhereArr.forEach((andWhere) => {
          let orArr
          let andArr

          if (andWhere[0].type === 'and' || andWhere[0].type === 'or') {
            andWhere.forEach((subAndWhere) => {
              if (subAndWhere.type === 'and') {
                andArr = subAndWhere.values
              }
              if (subAndWhere.type === 'or') {
                orArr = subAndWhere.values
              }
            })

            if (orArr && orArr.length > 0) {
              builder.where(
                typeof andArr[0] === 'object' ? exp.parseExpression(andArr[0]) : andArr[0],
                andArr[1],
                andArr[2] === null ? andArr[2] :
                  (typeof andArr[2] === 'object' ? exp.parseExpression(andArr[2]) : andArr[2]),
              ).orWhere(
                typeof orArr[0] === 'object' ? exp.parseExpression(orArr[0]) : orArr[0],
                orArr[1],
                orArr[2] === null ? orArr[2] :
                  (typeof orArr[2] === 'object' ? exp.parseExpression(orArr[2]) : orArr[2]),
              )
            }
          } else if (andWhere[0].type === 'operator') {
            builder.whereRaw(exp.parseExpression(andWhere[0]))
          } else {
            const [argA, operator, argB] = andWhere
            builder.where(
              typeof argA === 'object' ? exp.parseExpression(argA) : argA,
              operator,
              argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
            )
          }
        })
      }
    })
    .orWhere((builder) => {
      if (orWhereArr.length > 0) {
        orWhereArr.forEach((orWhere) => {
          if (orWhere[0].type === 'or') {
            const andArr = []

            orWhere.forEach((subOrWhere) => {
              andArr.push(subOrWhere.values)
            })

            andArr.forEach((subAndWhere) => {
              const [argA, operator, argB] = subAndWhere
              builder.where(
                typeof argA === 'object' ? exp.parseExpression(argA) : argA,
                operator,
                argB === null ? argB :
                  (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
              )
            })
          } else if (orWhere[0].type === 'operator') {
            builder.orWhere(exp.parseExpression(orWhere[0]))
          } else {
            const [argA, operator, argB] = orWhere
            builder.orWhere(
              typeof argA === 'object' ? exp.parseExpression(argA) : argA,
              operator,
              argB === null ? argB : (typeof argB === 'object' ? exp.parseExpression(argB) : argB),
            )
          }
        })
      }
    })

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


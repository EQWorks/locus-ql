/* eslint-disable no-use-before-define */
const {
  isNull,
  isNonNull,
  isArray,
  isInt,
  sanitizeString,
  escapeIdentifier,
  parserError,
  expressionTypes,
} = require('../utils')
const {
  parseExpression,
  parseViewExpression,
  parseCTEExpression,
  parseJoinExpression,
} = require('./expression')
const BaseNode = require('./base')


class SelectNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const {
      with: ctes,
      from,
      joins,
      distinct,
      columns,
      where,
      having,
      groupBy,
      orderBy,
      limit,
      offset,
    } = exp

    this._context = {
      ...this._context,
      ctes: { ...(this._context.ctes || {}) }, // shallow copy to prevent from adding to parent
      refs: {}, // views/subs/cte's in use in scope
    }

    // WITH
    this.with = []
    if (isNonNull(ctes)) {
      if (!isArray(ctes)) {
        throw parserError(`Invalid with syntax: ${ctes}`)
      }
      this.with = ctes.map(e => parseCTEExpression(e, this._context))
    }

    // FROM
    // string, select object or undefined/null (e.g. select true)
    this.from = undefined
    if (isNonNull(from)) {
      this.from = parseViewExpression(from, this._context)
    }

    // JOINS
    this.joins = []
    if (isNonNull(joins)) {
      if (!isArray(joins)) {
        throw parserError(`Invalid join syntax: ${joins}`)
      }
      this.joins = joins.map(e => parseJoinExpression(e, this._context))
    }

    // DISTINCT
    this.distinct = false
    if (isNonNull(distinct)) {
      if (typeof distinct !== 'boolean') {
        throw parserError(`Invalid distinct syntax: ${distinct}`)
      }
      this.distinct = distinct
    }

    // COLUMNS
    if (!isArray(columns, { minLength: 1 })) {
      throw parserError('Missing columns in select expression')
    }
    this.columns = columns.map(e => parseExpression(e, this._context))

    // WHERE
    this.where = []
    if (isNonNull(where)) {
      if (!isArray(where)) {
        console.log('where', where)
        throw parserError(`Invalid where syntax: ${where}`)
      }
      this.where = where.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in where expression: ${value.as || value._as}`)
        }
        return value
      })
    }

    // HAVING
    this.having = []
    if (isNonNull(having)) {
      if (!isArray(having)) {
        throw parserError(`Invalid having syntax: ${having}`)
      }
      this.having = having.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in having expression: ${value.as || value._as}`)
        }
        return value
      })
    }

    // GROUP BY
    this.groupBy = []
    if (isNonNull(groupBy)) {
      if (!isArray(groupBy)) {
        throw parserError(`Invalid groupBy syntax: ${groupBy}`)
      }
      this.groupBy = groupBy.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in group by expression: ${value.as || value._as}`)
        }
        return value
      })
    }

    // ORDER BY
    this.orderBy = []
    if (isNonNull(orderBy)) {
      if (!isArray(orderBy)) {
        throw parserError(`Invalid orderBy syntax: ${orderBy}`)
      }
      this.orderBy = orderBy.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in order by expression: ${value.as || value._as}`)
        }
        return value
      })
    }

    // LIMIT
    this.limit = undefined
    if (isNonNull(limit) && sanitizeString(limit) !== 'all') {
      if (!isInt(limit, 0)) {
        throw parserError(`Invalid limit: ${limit}`)
      }
      this.limit = limit
    }

    // OFFSET
    this.offset = undefined
    if (isNonNull(offset)) {
      if (!isInt(offset, 0)) {
        throw parserError(`Invalid offset: ${offset}`)
      }
      this.offset = offset
    }
  }

  _toSQL(options) {
    const ctes = this.with.length
      ? `WITH ${this.with.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const distinct = this.distinct ? 'DISTINCT' : ''
    const columns = this.columns.map(e => e.toSQL(options)).join(', ')

    const from = this.from ? `FROM ${this.from.toSQL(options)}` : ''

    const joins = this.joins.length
      ? this.joins.map(e => e.toSQL(options)).join(', ')
      : ''

    const where = this.where.length ?
      `WHERE ${this.where.map(e => e.toSQL(options)).join(' AND ')}`
      : ''

    const having = this.having.length
      ? `HAVING ${this.having.map(e => e.toSQL(options)).join(' AND ')}`
      : ''

    const groupBy = this.groupBy.length
      ? `GROUP BY ${this.groupBy.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const orderBy = this.orderBy.length
      ? `ORDER BY ${this.orderBy.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const limit = this.limit !== undefined ? `LIMIT ${this.limit}` : ''
    const offset = this.offset !== undefined ? `OFFSET ${this.offset}` : ''

    const sql = `
      ${ctes}
      SELECT ${distinct}
        ${columns}
      ${from}
      ${joins}
      ${where}
      ${groupBy}
      ${having}
      ${orderBy}
      ${limit}
      ${offset}
    `

    return this.isRoot() && !this.as && !this.cast ? sql : `(${sql})`
  }

  _toQL(options) {
    return {
      type: expressionTypes.SELECT,
      with: this.with.length ? this.with.map(e => e.toQL(options)) : undefined,
      from: this.from ? this.from.toQL(options) : undefined,
      joins: this.joins.length ? this.joins.map(e => e.toQL(options)) : undefined,
      distinct: this.distinct || undefined,
      columns: this.columns.map(e => e.toQL(options)),
      where: this.where.length ? this.where.map(e => e.toQL(options)) : undefined,
      having: this.having.length ? this.having.map(e => e.toQL(options)) : undefined,
      groupBy: this.groupBy.length ? this.groupBy.map(e => e.toQL(options)) : undefined,
      orderBy: this.orderBy.length ? this.orderBy.map(e => e.toQL(options)) : undefined,
      limit: this.limit,
      offset: this.offset,
    }
  }
}

class CTESelectNode extends SelectNode {
  constructor(exp, context) {
    // const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing with alias: ${as}`)
    }
    // register cte against parent context
    this._registerCTE(as)
    this._aliasIsUpdatable = false
  }

  _applyAliasToSQL(sql) {
    return `${escapeIdentifier(this.as)} AS ${sql}`
  }
}
CTESelectNode.castable = false

class RangeSelectNode extends SelectNode {
  constructor(exp, context) {
    // const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing subquery alias: ${as}`)
    }
    // register identifier against parent context
    this._registerRef(as)
    this._aliasIsUpdatable = false
  }
}
RangeSelectNode.castable = false

module.exports = { SelectNode, RangeSelectNode, CTESelectNode }

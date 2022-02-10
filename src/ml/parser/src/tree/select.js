const {
  isNull,
  isNonNull,
  isArray,
  isInt,
  isObjectExpression,
  sanitizeString,
  escapeIdentifier,
  parserError,
  getSourceContext,
} = require('../utils')
const { expressionTypes } = require('../types')
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
      operator, // union, intersect, except
      operands,
      with: ctes,
      from,
      joins,
      distinct,
      columns,
      where,
      having,
      groupBy, // TODO: able to reference output column
      orderBy, // TODO: able to reference output column
      limit,
      offset,
    } = exp

    // OPERATOR
    this.operator = undefined
    if (isNonNull(operator)) {
      this.operator = sanitizeString(operator, true)
      if (!this.operator || !['union', 'except', 'intersect'].includes(this.operator)) {
        throw parserError(`Invalid operator syntax: ${operator}`)
      }
    }
    // OPERANDS
    if (this.operator ? !isArray(operands, { length: 2 }) : isNonNull(operands)) {
      throw parserError(`Invalid operands syntax: ${operands}`)
    }
    this.operands = (operands || []).map((e) => {
      if (!isObjectExpression(e, expressionTypes.SELECT)) {
        throw parserError(`Invalid select operand: ${JSON.stringify(e)}`)
      }
      return parseExpression(e, this._context)
    })

    // WITH
    this.with = []
    if (isNonNull(ctes)) {
      if (this.operator || !isArray(ctes)) {
        throw parserError(`Invalid with syntax: ${ctes}`)
      }
      this.with = ctes.map(e => parseCTEExpression(e, this._context))
    }

    // FROM
    // string, select object or undefined/null (e.g. select true)
    this.from = undefined
    if (isNonNull(from)) {
      if (this.operator) {
        throw parserError(`Invalid from syntax: ${ctes}`)
      }
      this.from = parseViewExpression(from, this._context)
    }

    // JOINS
    this.joins = []
    if (isNonNull(joins)) {
      if (this.operator || !isArray(joins)) {
        throw parserError(`Invalid join syntax: ${joins}`)
      }
      this.joins = joins.map(e => parseJoinExpression(e, this._context))
    }

    // DISTINCT
    this.distinct = this.operator !== undefined
    if (isNonNull(distinct)) {
      if (typeof distinct !== 'boolean') {
        throw parserError(`Invalid distinct syntax: ${distinct}`)
      }
      this.distinct = distinct
    }

    // COLUMNS
    if (this.operator ? isNonNull(columns) : !isArray(columns, { minLength: 1 })) {
      throw parserError('Missing columns in select expression')
    }
    this.columns = (columns || []).map(e => parseExpression(e, this._context))

    // WHERE
    this.where = []
    if (isNonNull(where)) {
      if (this.operator || !isArray(where)) {
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
      if (this.operator || !isArray(having)) {
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
      if (this.operator || !isArray(groupBy)) {
        throw parserError(`Invalid groupBy syntax: ${groupBy}`)
      }
      this.groupBy = groupBy.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in groupBy expression: ${value.as || value._as}`)
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
          throw parserError(`Invalid alias in orderBy expression: ${value.as || value._as}`)
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

  _initContext(context) {
    super._initContext(context)
    // shallow copies to prevent from adding to parent
    this._context.ctes = { ...(this._parentContext.ctes || {}) } // all inherited + preceding cte's
    this._context.refs = { ...(this._parentContext.refs || {}) } // all inherited + preceding refs
  }

  _toSQL(options) {
    const orderBy = this.orderBy.length
      ? `ORDER BY ${this.orderBy.map(e => e.toSQL(options)).join(', ')}`
      : ''
    const limit = this.limit !== undefined ? `LIMIT ${this.limit}` : ''
    const offset = this.offset !== undefined ? `OFFSET ${this.offset}` : ''

    // has operator
    if (this.operator) {
      const operator = ` ${this.operator.toUpperCase()} ${!this.distinct ? 'ALL ' : ''}`
      const sql = `
        ${this.operands.map(e => e.toSQL(options)).join(operator)}
        ${orderBy}
        ${limit}
        ${offset}
      `
      return this.isRoot() && !this.as && !this.cast ? sql : `(${sql})`
    }

    // no operator
    const ctes = this.with.length
      ? `WITH ${this.with.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const distinct = this.distinct ? ' DISTINCT' : ''
    const columns = this.columns.map(e => e.toSQL(options)).join(', ')

    const from = this.from ? `FROM ${this.from.toSQL(options)}` : ''

    const joins = this.joins.length
      ? this.joins.map(e => e.toSQL(options)).join(' ')
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

    const sql = `
      ${ctes}
      SELECT${distinct}
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
      operator: this.operator,
      operands: this.operands.length ? this.operands.map(e => e.toQL(options)) : undefined,
      with: this.with.length ? this.with.map(e => e.toQL(options)) : undefined,
      from: this.from ? this.from.toQL(options) : undefined,
      joins: this.joins.length ? this.joins.map(e => e.toQL(options)) : undefined,
      distinct: this.distinct || undefined,
      columns: this.joins.length ? this.columns.map(e => e.toQL(options)) : undefined,
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

  _initContext(context) {
    super._initContext(context)
    const parentContext = getSourceContext(this._parentContext, 'refs')._parentContext
    this._context.refs = { ...((parentContext && parentContext.refs) || {}) } // only inherited refs
  }
}
RangeSelectNode.castable = false

class LateralRangeSelectNode extends RangeSelectNode {
  _initContext(context) {
    super._initContext(context)
    // all inherited + preceding refs
    this._context.refs = { ...(this._parentContext.refs || {}) }
  }
}

module.exports = { SelectNode, CTESelectNode, RangeSelectNode, LateralRangeSelectNode }

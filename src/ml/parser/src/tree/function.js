const { isArray, isNonNull, isEmptyOrNullArray, sanitizeString, parserError } = require('../utils')
const { expressionTypes } = require('../types')
const functions = require('../functions')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class FunctionNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid function syntax: ${JSON.stringify(exp)}`)
    }
    const { values: [name, ...args], distinct, where, orderBy } = exp
    this.name = sanitizeString(name)
    const fn = functions[this.name]
    if (!fn) {
      throw parserError(`Invalid function: ${name}`)
    }
    const { argsLength, minArgsLength, maxArgsLength, defaultCast, validate, isAggregate } = fn
    // function arguments
    this.args = (args || []).map((e) => {
      const arg = parseExpression(e, this._context)
      if (arg.as || arg._as) {
        throw parserError(`Invalid alias in function argument: ${arg.as || arg._as}`)
      }
      return arg
    })
    if (
      argsLength !== undefined
        ? this.args.length !== argsLength
        : (
          (minArgsLength && this.args.length < minArgsLength)
          || (maxArgsLength !== undefined && this.args.length > maxArgsLength)
        )
    ) {
      throw parserError(`Too few or too many arguments in function: ${this.name}`)
    }
    // casting
    this.defaultCast = defaultCast
    // aggregate function - distinct
    if (isNonNull(distinct) && (typeof exp.distinct !== 'boolean' || !isAggregate)) {
      throw parserError(`Invalid distinct syntax in function: ${this.name}`)
    }
    this.distinct = distinct || undefined
    // aggregate function - filter
    this.where = []
    if (isNonNull(where) && !isEmptyOrNullArray(where)) {
      if (!isAggregate || !isArray(where)) {
        throw parserError(`Invalid where syntax in function: ${this.name}`)
      }
      this.where = where.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in where expression: ${value.as || value._as}`)
        }
        return value
      })
    }
    // aggregate function - order by
    this.orderBy = []
    if (isNonNull(orderBy) && !isEmptyOrNullArray(orderBy)) {
      if (!isAggregate || !isArray(orderBy)) {
        throw parserError(`Invalid orderBy syntax in function: ${this.name}`)
      }
      this.orderBy = orderBy.map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in orderBy expression: ${value.as || value._as}`)
        }
        return value
      })
    }
    // illegal to use distinct and order by clause together
    if (this.distinct && this.orderBy.length) {
      throw parserError(`Cannot use distinct with orderBy in function: ${this.name}`)
    }
    // function-specific validation
    if (validate) {
      validate(this)
    }
  }

  _toSQL(options) {
    const args = this.args.map(e => e.toSQL(options)).join(', ')
    const distinct = this.distinct ? 'DISTINCT ' : ''
    const where = this.where.length ?
      ` FILTER (WHERE ${this.where.map(e => e.toSQL(options)).join(' AND ')})`
      : ''
    const orderBy = this.orderBy.length
      ? ` ORDER BY ${this.orderBy.map(e => e.toSQL(options)).join(', ')}`
      : ''
    const sql = `${this.name}(${distinct}${args}${orderBy})${where}`
    // return block
    return !where || (this.isRoot() && !this.as && !this.cast) ? sql : `(${sql})`
  }

  _applyCastToSQL(sql) {
    const cast = this.cast || this.defaultCast
    return cast ? `CAST(${sql} AS ${cast})` : sql
  }

  _toQL(options) {
    return {
      type: expressionTypes.FUNCTION,
      values: [this.name, ...this.args.map(e => e.toQL(options))],
      distinct: this.distinct,
      where: this.where.length ? this.where.map(e => e.toQL(options)) : undefined,
      orderBy: this.orderBy.length ? this.orderBy.map(e => e.toQL(options)) : undefined,
    }
  }

  _toShort(options) {
    return {
      name: 'function',
      args: {
        name: this.name,
        args: this.args.map(e => e.toShort(options)),
        distinct: this.distinct,
        where: this.where.length ? this.where.map(e => e.toShort(options)) : undefined,
        order_by: this.orderBy.length ? this.orderBy.map(e => e.toShort(options)) : undefined,
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = FunctionNode

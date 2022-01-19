const { isArray, sanitizeString, parserError, wrapSQL, expressionTypes } = require('../utils')
const functions = require('../functions')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class FunctionNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid function syntax: ${JSON.stringify(exp)}`)
    }
    const [name, ...args] = exp.values
    this.name = sanitizeString(name)
    const fn = functions[this.name]
    if (!fn) {
      throw parserError(`Invalid function: ${name}`)
    }
    this.args = (args || []).map((e) => {
      const arg = parseExpression(e, this._context)
      if (arg.as || arg._as) {
        throw parserError(`Invalid alias in function argument: ${arg.as || arg._as}`)
      }
      return arg
    })
    const { argsLength, minArgsLength, maxArgsLength, defaultCast } = fn
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
    this.defaultCast = defaultCast
  }

  _toSQL(options) {
    return `${this.name}(${this.args.map(e => e.toSQL(options)).join(', ')})`
  }

  _applyCastToSQL(sql) {
    const cast = this.cast || this.defaultCast
    return cast ? `CAST(${wrapSQL(sql)} AS ${cast})` : sql
  }

  _toQL(options) {
    return {
      type: expressionTypes.FUNCTION,
      values: [this.name, ...this.args.map(e => e.toQL(options))],
    }
  }

  _toShort(options) {
    return {
      name: 'function',
      args: {
        name: this.name,
        args: this.args.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = FunctionNode

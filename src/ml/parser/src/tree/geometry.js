const { isArray, sanitizeString, parserError, expressionTypes } = require('../utils')
const { geometries } = require('../geometries')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class GeometryNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 2 })) {
      throw parserError(`Invalid geometry syntax: ${JSON.stringify(exp)}`)
    }
    const [type, ...args] = exp.values
    this.type = sanitizeString(type)
    const geometry = geometries[this.type]
    if (!geometry) {
      throw parserError(`Invalid function: ${type}`)
    }
    this.args = args.map((e) => {
      const arg = parseExpression(e, this._context)
      if (arg.as || arg._as) {
        throw parserError(`Invalid alias in geometry argument: ${arg.as || arg._as}`)
      }
      return arg
    })
    const { argsLength, minArgsLength, maxArgsLength } = geometry
    if (
      argsLength !== undefined
        ? this.args.length !== argsLength
        : (
          (minArgsLength && this.args.length < minArgsLength)
          || (maxArgsLength !== undefined && this.args.length > maxArgsLength)
        )
    ) {
      throw parserError(`Too few or too many arguments in geometry: ${this.type}`)
    }
  }

  _toSQL(options) {
    return `@geo('${this.type}', [${this.args.map(e => e.toSQL(options)).join(', ')}])`
  }

  _toQL(options) {
    return {
      type: expressionTypes.GEOMETRY,
      values: [this.type, ...this.args.map(e => e.toQL(options))],
    }
  }

  _toShort(options) {
    return {
      name: 'geo',
      args: {
        type: this.type,
        args: this.args.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = GeometryNode

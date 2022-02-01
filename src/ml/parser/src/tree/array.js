const { isArray, parserError } = require('../utils')
const { expressionTypes } = require('../types')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class ArrayNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid array syntax: ${JSON.stringify(exp)}`)
    }
    this.values = exp.values.map((e) => {
      const value = parseExpression(e, this._context)
      if (value.as || value._as) {
        throw parserError(`Invalid alias in array expression: ${value.as || value._as}`)
      }
      return value
    })
  }

  _toSQL(options) {
    return `ARRAY[${this.values.map(e => e.toSQL(options)).join(', ')}]`
  }

  _toQL(options) {
    return {
      type: expressionTypes.ARRAY,
      values: this.values.map(e => e.toQL(options)),
    }
  }

  _toShort(options) {
    return {
      name: 'array',
      args: {
        values: this.values.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}
ArrayNode.castable = false

module.exports = ArrayNode

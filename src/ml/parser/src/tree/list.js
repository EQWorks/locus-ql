const { isArray, parserError, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class ListNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid list syntax: ${JSON.stringify(exp)}`)
    }
    this.values = exp.values.map((e) => {
      const value = parseExpression(e, this._context)
      if (value.as || value._as) {
        throw parserError(`Invalid alias in list expression: ${value.as || value._as}`)
      }
      return value
    })
  }

  _toSQL(options) {
    return `(${this.values.map(e => e.toSQL(options)).join(', ')})`
  }

  _toQL(options) {
    return {
      type: expressionTypes.LIST,
      values: this.values.map(e => e.toQL(options)),
    }
  }

  _toShort(options) {
    return {
      name: 'list',
      args: {
        values: this.values.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}
ListNode.castable = false

module.exports = ListNode

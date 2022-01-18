const { isNull, parserError, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class CastNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (isNull(this.cast) || value === undefined) {
      throw parserError(`Invalid casting syntax: ${JSON.stringify(exp)}`)
    }
    this.value = parseExpression(value, this._context)
    // fold into underlying value if possible
    if (this.value._applyCastAndAliasLayer(this.cast, this.as)) {
      return this.value
    }
  }

  _toSQL(options) {
    return this.value.toSQL(options)
  }

  _toQL(options) {
    return {
      type: expressionTypes.CAST,
      value: this.value.toQL(options),
    }
  }

  _toShort(options) {
    return {
      name: 'cast',
      args: {
        value: this.value.toShort(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = CastNode

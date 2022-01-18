const { escapeLiteral, parserError, expressionTypes } = require('../utils')
const BaseNode = require('./base')


// trino compatibility issues may arise when using primitive + casting
// e.g. cast('1 month' as interval) vs. interval '1' month
class PrimitiveNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!['string', 'boolean', 'number'].includes(typeof value) && value !== null) {
      throw parserError(`Invalid primitive: ${value}`)
    }
    this.value = value
  }

  _toSQL() {
    return typeof this.value === 'string' ? escapeLiteral(this.value) : String(this.value)
  }

  _toQL() {
    if (this.as || this.cast) {
      return {
        type: expressionTypes.PRIMITIVE,
        value: this.value,
      }
    }
    return this.value
  }

  _toShort(options) {
    return {
      name: 'primitive',
      args: {
        value: this.value.toShort(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = PrimitiveNode

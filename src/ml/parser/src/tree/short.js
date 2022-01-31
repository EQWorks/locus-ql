const { isString, parserError, expressionTypes } = require('../utils')
const { parseShortExpression, sanitizeShortExpression } = require('../short')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class ShortNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true)) {
      throw parserError(`Invalid short expression syntax: ${JSON.stringify(exp)}`)
    }
    // parse from short first - returns object expression
    const qlValue = parseShortExpression(value)
    this.value = parseExpression(qlValue, this._context)
    this.short = sanitizeShortExpression(value)
    this.value._validateCastAndAliasLayer(this.cast, this.as)
    this._populateCastAndAliasProxies(this.value)
  }

  _toSQL(options) {
    return (!options.keepParamRefs && this.parameters.size > 0) || !options.keepShorts
      ? this.value.toSQL(options)
      : this.short
  }

  _toQL(options) {
    if ((!options.keepParamRefs && this.parameters.size > 0) || !options.keepShorts) {
      return this.value.toQL(options)
    }
    if (!this.as && !this.cast) {
      return this.short
    }
    return {
      type: expressionTypes.SHORT,
      value: this.short,
    }
  }

  _toShort(options) {
    if (!options.keepParamRefs && this.parameters.size > 0) {
      if (this.as || this.cast) {
        throw parserError('Cannot push cast and alias values into short expression')
      }
      return this.value.toShort(options)
    }
    return this.short
  }
}

module.exports = ShortNode

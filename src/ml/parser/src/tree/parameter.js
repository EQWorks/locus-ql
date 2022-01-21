const { isString, parserError, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class ParameterReferenceNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true) || value.startsWith('__')) {
      throw parserError(`Invalid parameter: ${value}`)
    }
    this.name = value.toLowerCase()
    this._registerParam(this.name)
    if (!context.options.parameters) {
      this.value = undefined
      return
    }
    if (!(this.name in context.options.parameters)) {
      throw parserError(`Missing parameter value: ${this.name}`)
    }
    this.value = parseExpression(context.options.parameters[this.name], this._context)
    this.value._validateCastAndAliasLayer(this.cast, this.as)
    this._populateCastAndAliasProxies(this.value)
  }

  _toSQL(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toSQL(options)
    }
    return `@param('${this.name}')`
  }

  _toQL(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toQL(options)
    }
    return {
      type: expressionTypes.PARAMETER,
      value: this.name,
    }
  }

  _toShort(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toShort(options)
    }
    return {
      name: 'param',
      args: {
        name: this.name,
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = ParameterReferenceNode

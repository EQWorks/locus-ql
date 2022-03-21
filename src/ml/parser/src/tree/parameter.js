const { isString, parserError } = require('../utils')
const { expressionTypes } = require('../types')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class ParameterReferenceNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { name, defaultValue } = exp
    if (!isString(name, true) || name.startsWith('__')) {
      throw parserError(`Invalid parameter: ${name}`)
    }
    this.name = name.toLowerCase()
    this._registerParam(this.name)
    // default value
    this.hasDefaultValue = defaultValue !== undefined // has user-supplied default
    this.defaultValue = parseExpression(this.hasDefaultValue ? defaultValue : null, this._context)
    // no parameter value supplied
    if (!this._context.options.parameters || !(this.name in this._context.options.parameters)) {
      if (this._context.options.paramsMustHaveValues && !this.hasDefaultValue) {
        throw parserError(`Missing parameter value: ${this.name}`)
      }
      this.value = undefined
    } else {
      this.value = parseExpression(this._context.options.parameters[this.name], this._context)
    }
    const value = this.value !== undefined ? this.value : this.defaultValue
    value._validateCastAndAliasLayer(this.cast, this.as)
    this._populateCastAndAliasProxies(value)
  }

  _toSQL(options) {
    // substitute with param value
    if (!options.keepParamRefs) {
      const value = this.value !== undefined ? this.value : this.defaultValue
      return value.toSQL(options)
    }
    const defaultValue = this.hasDefaultValue
      ? `, default_value=@sql(sql=${this.defaultValue.toSQL(options)})`
      : ''
    return `@param(name='${this.name}'${defaultValue})`
  }

  _toQL(options) {
    // substitute with param value
    if (!options.keepParamRefs) {
      const value = this.value !== undefined ? this.value : this.defaultValue
      return value.toQL(options)
    }
    return {
      type: expressionTypes.PARAMETER,
      name: this.name,
      defaultValue: this.hasDefaultValue ? this.defaultValue.toQL(options) : undefined,
    }
  }

  _toShort(options) {
    // substitute with param value
    if (!options.keepParamRefs) {
      const value = this.value !== undefined ? this.value : this.defaultValue
      return value.toShort(options)
    }
    return {
      name: 'param',
      args: {
        name: this.name,
        default_value: this.hasDefaultValue ? this.defaultValue.toShort(options) : undefined,
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = ParameterReferenceNode

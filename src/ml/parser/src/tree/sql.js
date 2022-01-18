const { isString, parserError, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const { parseSQLExpression } = require('../sql')
const BaseNode = require('./base')


class SQLNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true)) {
      throw parserError(`Invalid sql syntax: ${JSON.stringify(exp)}`)
    }
    // parse from sql first
    const qlValue = parseSQLExpression(value)
    this.value = parseExpression(qlValue, this._context)
    // fold into underlying value if possible
    if (this.value._applyCastAndAliasLayer(this.cast, this.as)) {
      return this.value
    }
    this._populateCastAndAliasProxies(this.value)
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
      name: 'sql',
      args: {
        sql: this.value.toSQL(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = SQLNode

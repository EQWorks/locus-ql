const { isNonNull, sanitizeString, parserError, wrapSQL, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class SortNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { value, direction, nulls } = exp
    if (!value || value === true) {
      throw parserError(`Invalid sorting syntax: ${JSON.stringify(exp)}`)
    }
    this.direction = undefined
    if (isNonNull(direction)) {
      const safeDirection = sanitizeString(direction)
      if (!['asc', 'desc'].includes(safeDirection)) {
        throw parserError(`Invalid sorting direction syntax: ${JSON.stringify(exp)}`)
      }
      this.direction = safeDirection
    }
    this.nulls = undefined
    if (isNonNull(nulls)) {
      const safeNulls = sanitizeString(nulls)
      if (!['first', 'last'].includes(safeNulls)) {
        throw parserError(`Invalid sorting nulls syntax: ${JSON.stringify(exp)}`)
      }
      this.nulls = safeNulls
    }
    this.value = parseExpression(value, this._context)
    if (this.value.as || this.value._as) {
      throw parserError(`Invalid alias in sorting expression: ${this.value.as || this.value._as}`)
    }
  }

  _toSQL(options) {
    const direction = this.direction ? ` ${this.direction}` : ''
    const nulls = this.nulls ? ` NULLS ${this.nulls}` : ''
    return wrapSQL(this.value.toSQL(options)) + direction + nulls
  }

  _toQL(options) {
    return {
      type: expressionTypes.SORT,
      value: this.value.toQL(options),
      direction: this.direction,
      nulls: this.nulls,
    }
  }
}
SortNode.aliasable = false
SortNode.castable = false

module.exports = SortNode

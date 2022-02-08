const { sanitizeString, parserError } = require('../utils')
const { expressionTypes, joinTypes, joinTypeValues } = require('../types')
const { parseExpression, parseViewExpression, parseLateralViewExpression } = require('./expression')
const BaseNode = require('./base')


class JoinNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { joinType, view, on } = exp
    this.joinType = sanitizeString(joinType)
    if (!(this.joinType in joinTypeValues)) {
      throw parserError(`Invalid join type: ${joinType}`)
    }
    this.view = this.joinType === joinTypes.LATERAL
      ? parseLateralViewExpression(view, this._context)
      : parseViewExpression(view, this._context)
    if ([joinTypes.CROSS, joinTypes.LATERAL].includes(this.joinType)) {
      this.on = undefined
      return
    }
    this.on = parseExpression(on, this._context)
    if (this.on.as || this.on._as) {
      throw parserError(`Invalid alias in join condition: ${this.on.as || this.on._as}`)
    }
  }

  _toSQL(options) {
    const view = this.view.toSQL(options)
    if (this.joinType === joinTypes.LATERAL) {
      return `CROSS JOIN LATERAL ${view}`
    }
    const joinType = this.joinType.toUpperCase()
    return `${joinType} JOIN ${view}${this.on !== undefined ? `ON ${this.on.toSQL(options)}` : ''}`
  }

  _toQL(options) {
    return {
      type: expressionTypes.JOIN,
      joinType: this.joinType,
      view: this.view.toQL(options),
      on: this.on !== undefined ? this.on.toQL(options) : undefined,
    }
  }
}
JoinNode.aliasable = false
JoinNode.castable = false

module.exports = JoinNode

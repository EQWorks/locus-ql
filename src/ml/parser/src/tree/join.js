const { sanitizeString, parserError, wrapSQL, expressionTypes } = require('../utils')
const { parseExpression, parseViewExpression } = require('./expression')
const BaseNode = require('./base')


class JoinNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { joinType, view, on } = exp
    if (!['inner', 'left', 'right'].includes(sanitizeString(joinType))) {
      throw parserError(`Invalid join type: ${joinType}`)
    }
    this.joinType = joinType
    this.view = parseViewExpression(view, this._context)
    this.on = parseExpression(on, this._context)
    if (this.on.as || this.on._as) {
      throw parserError(`Invalid alias in join condition: ${this.on.as || this.on._as}`)
    }
  }

  _toSQL() {
    return `${this.joinType} JOIN ${this.view.toSQL()} ON ${wrapSQL(this.on.toSQL())}`
  }

  _toQL(options) {
    return {
      type: expressionTypes.JOIN,
      joinType: this.joinType,
      view: this.view.toQL(options),
      on: this.on.toQL(options),
    }
  }
}
JoinNode.aliasable = false
JoinNode.castable = false

module.exports = JoinNode

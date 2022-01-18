const { isString, escapeIdentifier, parserError, expressionTypes } = require('../utils')
const BaseNode = require('./base')


class ViewReferenceNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { view, as } = exp
    // view must be accessible from local scope
    if (!isString(view, true)) {
      throw parserError(`Invalid view reference expression: ${view}`)
    }
    this.view = view
    this._registerRef(as || view, view)
    if (as) {
      this._aliasIsUpdatable = false
    }
  }

  _toSQL() {
    return escapeIdentifier(this.view)
  }

  _toQL() {
    return {
      type: expressionTypes.VIEW,
      view: this.view,
    }
  }

  _toShort() {
    return {
      name: 'view',
      args: {
        name: this.view,
        as: this.as,
      },
    }
  }
}
ViewReferenceNode.castable = false

module.exports = ViewReferenceNode

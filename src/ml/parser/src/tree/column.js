const {
  isNull,
  isNonNull,
  isString,
  escapeIdentifier,
  parserError,
  expressionTypes,
} = require('../utils')
const BaseNode = require('./base')


class ColumnReferenceNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    const { column, view, as } = exp
    if (
      !isString(column, true)
      || (
        this.hasSelectAncestor()
          ? !(isString(view, true) && view in this._context.refs)
            && !(isNull(view) && Object.keys(this._context.refs).length === 1)
          : !(isString(view, true))
      )
    ) {
      throw parserError(`Invalid column expression: ${JSON.stringify(exp)}`)
    }
    if (isNonNull(as) && column === '*') {
      throw parserError(`Invalid column alias: ${as}`)
    }
    if (as === column) {
      this.as = undefined
    }
    this.column = column
    this.view = isNull(view) ? Object.keys(this._context.refs)[0] : (view || undefined)
    // register view + column in context
    this._registerViewColumn(this.view, this.column)
  }

  _toSQL() {
    const column = this.column === '*' ? '*' : escapeIdentifier(this.column)
    return this.view ? `${escapeIdentifier(this.view)}.${column}` : column
  }

  _toQL() {
    return { type: expressionTypes.COLUMN, column: this.column, view: this.view }
  }

  _toShort() {
    return {
      name: 'column',
      args: {
        column: this.column,
        view: this.view,
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = ColumnReferenceNode

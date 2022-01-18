const { isNull, isArray, parserError, wrapSQL, expressionTypes } = require('../utils')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class CaseNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid case syntax: ${JSON.stringify(exp)}`)
    }
    const cases = [...exp.values]
    // first item is either the default result or a cond/res pair
    let defaultRes
    if (!isArray(cases[0])) {
      defaultRes = parseExpression(cases.shift(), this._context)
      if (defaultRes.as || defaultRes._as) {
        throw parserError(`Invalid alias in case expression: ${defaultRes.as || defaultRes._as}`)
      }
    }
    this.defaultRes = defaultRes
    this.cases = cases.map(([cond, res]) => {
      if (isNull(cond) || res === undefined) {
        throw parserError(`Invalid case syntax: ${JSON.stringify(exp)}`)
      }
      return [cond, res].map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in case expression: ${value.as || value._as}`)
        }
        return value
      })
    })
  }

  _toSQL(options) {
    return `
      CASE
        ${this.case
    .map(([cond, res]) =>
      `WHEN ${wrapSQL(cond.toSQL(options))} THEN ${wrapSQL(res.toSQL(options))}`)
    .join('\n')}
        ${this.defaultRes ? `ELSE ${wrapSQL(this.defaultRes.toSQL(options))}` : ''}
      END
    `
  }

  _toQL(options) {
    const cases = this.cases.map(c => c.map(e => e.toQL(options)))
    return {
      type: expressionTypes.CASE,
      values: this.defaultRes ? [this.defaultRes.toQL(options), ...cases] : cases,
    }
  }
}

module.exports = CaseNode

const { isString, isArray, sanitizeString, parserError } = require('../utils')
const { expressionTypes } = require('../types')
const operators = require('../operators')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class OperatorNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    // binary and left-unary operators
    if (
      !isArray(exp.values, { minLength: 2 })
      || !(isString(exp.values[0]) || isArray(exp.values[0], { length: 2 }))
    ) {
      throw parserError(`Invalid operator syntax: ${JSON.stringify(exp)}`)
    }
    const [operator, ...operands] = exp.values
    const [qualifier, name] = isArray(operator) ? operator : [undefined, operator];

    [qualifier, name].forEach((n, i) => {
      const key = i ? 'name' : 'qualifier'
      this[key] = sanitizeString(n)
      if (key === 'qualifier' && n === undefined) {
        return
      }
      const op = operators[this[key]]
      if (!op) {
        throw parserError(`Invalid operator: ${n}`)
      }
    })
    this.operands = operands.map((e) => {
      const operand = parseExpression(e, this._context)
      if (operand.as || operand._as) {
        throw parserError(`Invalid alias in operand: ${operand.as || operand._as}`)
      }
      return operand
    })
    const { opsLength, minOpsLength, maxOpsLength, validate } = operators[this.name]
    if (
      opsLength !== undefined
        ? this.operands.length !== opsLength
        : (
          (minOpsLength && this.operands.length < minOpsLength)
          || (maxOpsLength !== undefined && this.operands.length > maxOpsLength)
        )
    ) {
      throw parserError(`Too few or too many operands for operator: ${this.name}`)
    }
    if (validate) {
      validate(this)
    }
  }

  _toSQL(options) {
    let sql
    const { toSQL } = operators[this.name]
    if (toSQL) {
      sql = toSQL(this, options)
    } else {
      const operator = `${this.qualifier ? `${this.qualifier} ` : ''}${this.name} `.toUpperCase()
      sql = this.operands.map((o, i, all) => {
        const op = i > 0 || all.length === 1 ? operator : ''
        return op + o.toSQL(options)
      }).join(' ')
    }
    return this.isRoot() && !this.as && !this.cast ? sql : `(${sql})`
  }

  _toQL(options) {
    if ((this.name === 'and' || this.name === 'or') && !this.qualifier) {
      return {
        type: this.name === 'and' ? expressionTypes.AND : expressionTypes.OR,
        values: this.operands.map(e => e.toQL(options)),
      }
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [
        this.qualifier ? [this.qualifier, this.name] : this.name,
        ...this.operands.map(e => e.toQL(options)),
      ],
    }
  }

  _toShort(options) {
    if (this.name === 'and' || this.name === 'or') {
      return {
        name: this.name,
        args: {
          qualifier: this.qualifier,
          operands: this.operands.map(e => e.toShort(options)),
          as: this.as,
          cast: this.cast,
        },
      }
    }
    return {
      name: 'operator',
      args: {
        operator: this.name,
        qualifier: this.qualifier,
        operands: this.operands.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = OperatorNode

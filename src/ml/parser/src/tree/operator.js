const { isArray, sanitizeString, parserError, expressionTypes } = require('../utils')
const operators = require('../operators')
const { parseExpression } = require('./expression')
const BaseNode = require('./base')


class OperatorNode extends BaseNode {
  constructor(exp, context) {
    super(exp, context)
    // binary and left-unary operators
    if (!isArray(exp.values, { minLength: 2 })) {
      throw parserError(`Invalid operator syntax: ${JSON.stringify(exp)}`)
    }
    const [name, ...operands] = exp.values
    this.name = sanitizeString(name)
    const op = operators[this.name]
    if (!op) {
      throw parserError(`Invalid operator: ${name}`)
    }
    if ((this.name === 'between' || this.name === 'not between') && operands.length !== 3) {
      throw parserError(`Invalid number of operands for operator: ${this.name}`)
    }
    this.operands = operands.map((e) => {
      const operand = parseExpression(e, this._context)
      if (operand.as || operand._as) {
        throw parserError(`Invalid alias in operand: ${operand.as || operand._as}`)
      }
      return operand
    })
  }

  _toSQL(options) {
    if (this.name === 'between' || this.name === 'not between') {
      const [oA, oB, oC] = this.operands
      return `${oA.toSQL(options)} ${this.name} ${oB.toSQL(options)} AND ${oC.toSQL(options)}`
    }
    return this.operands.map((o, i, all) => {
      const op = i > 0 || all.length === 1 ? `${this.name} ` : ''
      return op + o.toSQL(options)
    }).join(' ')
  }

  _toQL(options) {
    if (this.name === 'and' || this.name === 'or') {
      return {
        type: this.name === 'and' ? expressionTypes.AND : expressionTypes.OR,
        values: this.operands.map(e => e.toQL(options)),
      }
    }
    return {
      type: expressionTypes.OPERATOR,
      values: [this.name, ...this.operands.map(e => e.toQL(options))],
    }
  }

  _toShort(options) {
    if (this.name === 'and' || this.name === 'or') {
      return {
        name: this.name,
        args: {
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
        operands: this.operands.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

module.exports = OperatorNode

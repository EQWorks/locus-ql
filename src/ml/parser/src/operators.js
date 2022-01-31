const { parserError } = require('./utils')


const operators = {
  // logic operators
  and: {},
  or: {},
  not: {},

  // comparison operators
  '>': {},
  '>=': {},
  '<': {},
  '<=': {},
  '=': {},
  '!=': {},
  '<>': {},
  in: {},
  'not in': {},
  like: {},
  'not like': {},
  is: {},
  'is not': {},
  'is of': {},
  'is not of': {},

  // string operators
  '||': {},

  // arithmatic operators
  '+': {},
  '-': {},
  '*': {},
  '/': {},
  '%': {},

  // JSON operators
  // 'json array element at': { value: '->' },
  // 'json object field with key': { value: '->' },
  // 'json array element as text at': { value: '->>' },
  // 'json object field as text with key': { value: '->>' },
  // 'json object at path': { value: '#>' },
  // 'json object as text at path': { value: '#>>' },
}

operators.between = {
  opsLength: 3,
  toSQL: (node, options) => {
    const [left, rLeft, rRight] = node.operands.map(o => o.toSQL(options))
    const operator = `${node.qualifier ? `${node.qualifier} ` : ''}${node.name} `.toUpperCase()
    return `${left} ${operator}${rLeft} AND ${rRight}`
  },
}
operators['not between'] = operators.between

operators.any = {
  opsLength: 2,
  validate: (node) => {
    if (!node.qualifier) {
      throw parserError(`Missing operator qualifier for operator: ${node.name}`)
    }
  },
  toSQL: (node, options) => {
    const [left, right] = node.operands.map(o => o.toSQL(options))
    return `${left} ${node.qualifier.toUpperCase()} ${node.name.toUpperCase()} (${right})`
  },
}
operators.some = operators.any
operators.all = operators.any

module.exports = operators

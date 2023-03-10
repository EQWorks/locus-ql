const { parserError } = require('./utils')


const operators = {
  // logical operators
  and: { minOpsLength: 2 },
  or: { minOpsLength: 2 },
  not: { minOpsLength: 1 },

  in: {
    opsLength: 2,
    qualifiers: ['not'],
  },
  'not in': { opsLength: 2 },
  like: {
    opsLength: 2,
    qualifiers: ['not'],
  },
  'not like': { opsLength: 2 },
  is: { opsLength: 2 },
  'is not': { opsLength: 2 },
  'is of': { opsLength: 2 },
  'is not of': { opsLength: 2 },
  exists: { opsLength: 1 },

  // string operators
  '||': { minOpsLength: 2 },

  // array operators
  '[]': { // subscript operator
    minOpsLength: 2,
    toSQL: (node, options) => {
      const [arr, ...ind] = node.operands.map(o => o.toSQL(options))
      return `(${arr})${ind.map(e => `[${e}]`).join('')}`
    },
  },

  // arithmatic operators
  '+': { minOpsLength: 2 },
  '-': { minOpsLength: 2 },
  '*': { minOpsLength: 2 },
  '/': { minOpsLength: 2 },
  '%': { minOpsLength: 2 },
  '^': { minOpsLength: 2 },

  // JSON operators
  // 'json array element at': { value: '->' },
  // 'json object field with key': { value: '->' },
  // 'json array element as text at': { value: '->>' },
  // 'json object field as text with key': { value: '->>' },
  // 'json object at path': { value: '#>' },
  // 'json object as text at path': { value: '#>>' },
}

// comparison operators
operators['>'] = { opsLength: 2 }
operators['>='] = { opsLength: 2 }
operators['<'] = { opsLength: 2 }
operators['<='] = { opsLength: 2 }
operators['='] = { opsLength: 2 }
operators['!='] = { opsLength: 2 }
operators['<>'] = { opsLength: 2 }
operators.between = {
  opsLength: 3,
  qualifiers: ['not'],
  toSQL: (node, options) => {
    const [left, rLeft, rRight] = node.operands.map(o => o.toSQL(options))
    const operator = `${node.qualifier ? `${node.qualifier} ` : ''}${node.name} `.toUpperCase()
    return `${left} ${operator}${rLeft} AND ${rRight}`
  },
}
operators['not between'] = { ...operators.between, qualifiers: [] }

// array operators
operators.any = {
  opsLength: 2,
  qualifiers: ['>', '>=', '<', '<=', '=', '!=', '<>', 'like', 'not like'],
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

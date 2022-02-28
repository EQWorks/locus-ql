const operators = {}

operators['^'] = {
  trino: (node, options) => {
    const [left, right] = node.operands.map(o => o.to('trino', options))
    return `pow(${left}, ${right})`
  },
}

operators['[]'] = {
  pg: (node, options) => {
    const [arr, ...ind] = node.operands.map(o => o.to('pg', options))
    return `(${arr})${ind.map(e => `[${e}]`).join('')}`
  },
  trino: (node, options) => {
    const [arr, ...ind] = node.operands.map(o => o.to('trino', options))
    return `(${arr})${ind.map(e => `[${e}]`).join('')}`
  },
}

operators.any = {
  pg: (node, options) => {
    const [left, right] = node.operands.map(o => o.to('pg', options))
    return `${left} ${node.qualifier} ${node.name} (${right})`
  },
  trino: (node, options) => {
    const [left, right] = node.operands.map(o => o.to('trino', options))
    return `${left} ${node.qualifier} ${node.name} (${right})`
  },
}
operators.some = operators.any
operators.all = operators.any

operators.between = {
  pg: (node, options) => {
    const [left, rLeft, rRight] = node.operands.map(o => o.to('pg', options))
    const operator = `${node.qualifier ? `${node.qualifier} ` : ''}${node.name} `
    return `${left} ${operator}${rLeft} AND ${rRight}`
  },
  trino: (node, options) => {
    const [left, rLeft, rRight] = node.operands.map(o => o.to('trino', options))
    const operator = `${node.qualifier ? `${node.qualifier} ` : ''}${node.name} `
    return `${left} ${operator}${rLeft} AND ${rRight}`
  },
}
operators['not between'] = operators.between

module.exports = operators

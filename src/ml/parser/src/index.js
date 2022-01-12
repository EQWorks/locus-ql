const { parseExpression, nodes } = require('./tree')
const { parseShortExpression } = require('./short')
const { parseSQLExpression } = require('./sql')


const parseQLToTree = parseExpression

const parseSQLToTree = (sql) => {
  const ql = parseSQLExpression(sql)
  return parseExpression(ql)
}

const parseShortToTree = (short) => {
  const ql = parseShortExpression(short)
  return parseExpression(ql)
}

module.exports = {
  parseQLToTree,
  parseSQLToTree,
  parseShortToTree,
  nodes,
}

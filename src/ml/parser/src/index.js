const { parseExpression, nodes } = require('./tree')
const { parseShortExpression } = require('./short')
const { parseSQLExpression } = require('./sql')
const { expressionTypes, castTypes, geometryTypes } = require('./types')


const parseQLToTree = (ql, options) => (
  options ? parseExpression(ql, { options }) : parseExpression(ql)
)

const parseSQLToTree = (sql, options) => {
  const ql = parseSQLExpression(sql)
  return parseQLToTree(ql, options)
}

const parseShortToTree = (short, options) => {
  const ql = parseShortExpression(short)
  return parseQLToTree(ql, options)
}

module.exports = {
  parseQLToTree,
  parseSQLToTree,
  parseShortToTree,
  nodes,
  expressionTypes,
  castTypes,
  geometryTypes,
}

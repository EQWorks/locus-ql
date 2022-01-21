const { useAPIErrorOptions } = require('../../util/api-error')
const { parseQLToTree, parseSQLToTree, nodes: { SelectNode } } = require('./src')
require('./pg') // init - attach parsers
require('./trino') // init - attach parsers


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const parseQueryToTree = (query, { type = 'ql', keepShorts = true, parameters } = {}) => {
  if (type !== 'ql' && type !== 'sql') {
    throw new Error('Unknown query type')
  }
  const options = { keepShorts, parameters }
  const tree = type === 'ql' ? parseQLToTree(query, options) : parseSQLToTree(query, options)
  if (!(tree instanceof SelectNode)) {
    throw apiError('Query must be of type select', 400)
  }
  return tree
}

const parseQueryTreeToEngine = (
  tree,
  { engine = 'pg', views = {}, whitelabelID, customerID } = {},
) => {
  if (engine !== 'trino' && engine !== 'pg') {
    throw new Error('Unknown engine')
  }
  // no order
  const queryViews = Object.keys(tree.viewColumns).reduce((acc, v) => {
    if (!(v in views)) {
      throw new Error(`Missing view: ${v}`)
    }
    acc[v] = views[v]
    return acc
  }, {})
  const query = tree.to(engine, { whitelabelID, customerID })
  if (!Object.keys(queryViews).length) {
    return query
  }
  return `
    WITH ${Object.entries(queryViews).map(([name, query]) => `"${name}" AS (${query})`).join(', ')}
    SELECT * FROM (${query}) q
  `
}

const parseQueryTreeToPG = (tree, { views, whitelabelID, customerID } = {}) =>
  parseQueryTreeToEngine(tree, { engine: 'pg', views, whitelabelID, customerID })

const parseQueryTreeToTrino = (tree, { views, whitelabelID, customerID } = {}) =>
  parseQueryTreeToEngine(tree, { engine: 'trino', views, whitelabelID, customerID })

module.exports = {
  parseQueryToTree,
  parseQueryTreeToEngine,
  parseQueryTreeToPG,
  parseQueryTreeToTrino,
}

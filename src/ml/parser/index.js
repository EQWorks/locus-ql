const { useAPIErrorOptions } = require('../../util/api-error')
const { parseQLToTree, parseSQLToTree, nodes: { SelectNode } } = require('./src')
const { isValidSQLExpression } = require('./src/sql')
const { ParserError } = require('./src/utils')
require('./pg') // init - attach parsers
require('./trino') // init - attach parsers


const {
  apiError,
  getSetAPIError,
} = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const makeRefSeq = () => {
  let i = 0
  return () => {
    i += 1
    return `__ref_${i}`
  }
}

const parseQueryToTree = (query, { type = 'ql', parameters, paramsMustHaveValues } = {}) => {
  if (type !== 'ql' && type !== 'sql') {
    throw new Error('Unknown query type')
  }
  const options = { parameters, paramsMustHaveValues }
  const tree = type === 'ql' ? parseQLToTree(query, options) : parseSQLToTree(query, options)
  // top-level node validation
  if (!(tree instanceof SelectNode) || tree.as || tree.cast) {
    throw apiError('Invalid query syntax', 400)
  }
  if (!Object.keys(tree.viewColumns).length) {
    // throw apiError('Query must use at least one view', 400)
  }
  // check that query makes sense using the sql parser
  const sql = tree.toSQL({ keepShorts: false, keepParamRefs: !paramsMustHaveValues })
  if (!isValidSQLExpression(sql)) {
    throw apiError('Invalid query syntax', 400)
  }
  return tree
}

const parseQueryTreeToEngine = (
  tree,
  { engine = 'pg', viewQueries = {}, whitelabelID, customerID, limit } = {},
) => {
  if (engine !== 'trino' && engine !== 'pg') {
    throw new Error('Unknown engine')
  }
  // no order
  const views = Object.keys(tree.viewColumns).reduce((acc, v) => {
    if (!(v in viewQueries)) {
      throw new Error(`Missing view: ${v}`)
    }
    acc[v] = viewQueries[v]
    return acc
  }, {})
  const query = tree.to(engine, { whitelabelID, customerID, getRef: makeRefSeq() })
  const viewCTEs = Object.keys(views).length
    ? `WITH ${Object.entries(views).map(([name, query]) => `"${name}" AS (${query})`).join(', ')}`
    : ''
  return `
    ${viewCTEs}
    SELECT * FROM (${query}) AS __query
    ${limit !== undefined ? `LIMIT ${limit}` : ''}
  `
}

const parseQueryTreeToPG = (tree, { viewQueries, whitelabelID, customerID } = {}) =>
  parseQueryTreeToEngine(tree, { engine: 'pg', viewQueries, whitelabelID, customerID })

const parseQueryTreeToTrino = (tree, { viewQueries, whitelabelID, customerID } = {}) =>
  parseQueryTreeToEngine(tree, { engine: 'trino', viewQueries, whitelabelID, customerID })

const parseQueryToTreeMW = ({
  onlyUseBodyQuery = false,
  paramsMustHaveValues = false,
  useBodyParameters = true,
} = {}) => async (req, _, next) => {
  try {
    // if a saved query or execution have been attached to req, use it
    // else use req.body
    const loadedQuery = !onlyUseBodyQuery && (req.ql.query || req.ql.execution)
    const { query } = loadedQuery || req.body
    const { sql, parameters } = req.body
    if (!query && !sql) {
      throw apiError('Missing field(s): query and/or sql')
    }
    req.ql.tree = parseQueryToTree(query || sql, {
      type: query ? 'ql' : 'sql',
      parameters: useBodyParameters ? parameters : undefined,
      paramsMustHaveValues,
    })
    next()
  } catch (err) {
    if (err instanceof ParserError) {
      return next(apiError(err.message, 400))
    }
    next(getSetAPIError(err, 'Failed to parse the query', 500))
  }
}

module.exports = {
  parseQueryToTree,
  parseQueryTreeToEngine,
  parseQueryTreeToPG,
  parseQueryTreeToTrino,
  parseQueryToTreeMW,
  ParserError,
}

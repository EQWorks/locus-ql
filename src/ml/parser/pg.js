const { useAPIErrorOptions } = require('../../util/api-error')
const {
  nodes: {
    ArrayNode,
    CaseNode,
    CastNode,
    ColumnReferenceNode,
    FunctionNode,
    GeometryNode,
    JoinNode,
    ListNode,
    OperatorNode,
    ParameterReferenceNode,
    PrimitiveNode,
    SelectNode,
    CTESelectNode,
    RangeSelectNode,
    ShortNode,
    SortNode,
    SQLNode,
    ViewReferenceNode,
  },
} = require('./src')
const functions = require('./functions')
const { wrapSQL, escapeIdentifier, escapeLiteral, trimSQL, ParserError } = require('./src/utils')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const withOptions = (parser, { alias = true, cast = true, trim = true } = {}) =>
  (node, options) => {
    try {
      let pg = parser(node, options)
      if (cast && node.cast) {
        pg = `CAST(${wrapSQL(pg)} AS ${node.cast})`
      }
      if (alias && node.as) {
        pg = `${wrapSQL(pg)} AS ${escapeIdentifier(node.as)}`
      }
      if (trim) {
        pg = trimSQL(pg)
      }
      return pg
    } catch (err) {
      if (err instanceof ParserError) {
        throw apiError(err.message, 400)
      }
      throw err
    }
  }


ArrayNode.registerParser('pg', withOptions((node, options) =>
  `ARRAY[${node.values.map(e => e.to('pg', options)).join(', ')}]`))

CaseNode.registerParser('pg', withOptions((node, options) => `
  CASE
    ${node.case
    .map(([cond, res]) =>
      `WHEN ${wrapSQL(cond.to('pg', options))} THEN ${wrapSQL(res.to('pg', options))}`)
    .join('\n')}
    ${node.defaultRes ? `ELSE ${wrapSQL(node.defaultRes.to('pg', options))}` : ''}
  END
`))

CastNode.registerParser('pg', withOptions((node, options) => node.value.to('pg', options)))

ColumnReferenceNode.registerParser('pg', withOptions((node) => {
  const column = node.column === '*' ? '*' : escapeIdentifier(node.column)
  return node.view ? `${escapeIdentifier(node.view)}.${column}` : column
}))

FunctionNode.registerParser('pg', withOptions((node, options) => {
  let name = { node }
  // function name is different or implementation is non-standard
  if (node.name in functions && 'pg' in functions[node.name]) {
    if (typeof functions[node.name].pg === 'function') {
      return functions[node.name].pg(node, options)
    }
    name = functions[node.name].pg
  }
  return `${name}(${node.args.map(e => e.to('pg', options)).join(', ')})`
}))

GeometryNode.registerParser('pg', withOptions((node, options) =>
  `'geo:${node.type}:' || ${node.args.map(e => wrapSQL(e.to('pg', options))).join(" || ':' || ")}`))

JoinNode.registerParser('pg', withOptions(node =>
  `${node.joinType} JOIN ${node.view.to('pg')} ON ${wrapSQL(node.on.to('pg'))}`))

ListNode.registerParser('pg', withOptions((node, options) =>
  `(${node.values.map(e => e.to('pg', options)).join(', ')})`))

OperatorNode.registerParser('pg', withOptions((node, options) => {
  if (node.name === 'between' || node.name === 'not between') {
    const [oA, oB, oC] = node.operands
    return `
      ${oA.to('pg', options)} ${node.name}
        ${oB.to('pg', options)} AND ${oC.to('pg', options)}
    `
  }
  return node.operands.map((o, i, all) => {
    const op = i > 0 || all.length === 1 ? `${node.name} ` : ''
    return op + o.to('pg', options)
  }).join(' ')
}))

ParameterReferenceNode.registerParser('pg', withOptions((node) => {
  if (node.value === undefined) {
    throw apiError('Missing parameter value', 400)
  }
  return node.value.to('pg')
}))

PrimitiveNode.registerParser('pg', withOptions(node =>
  (typeof node.value === 'string' ? escapeLiteral(node.value) : String(node.value))))

const selectParser = (node, options) => {
  const ctes = node.with.length
    ? `WITH ${node.with.map(e => e.to('pg', options)).join(', ')}`
    : ''

  const distinct = node.distinct ? 'DISTINCT' : ''
  const columns = node.columns
    .map(e => (e.as ? e.to('pg', options) : wrapSQL(e.to('pg', options))))
    .join(', ')

  const from = node.from ? `FROM ${node.from.to('pg', options)}` : ''

  const joins = node.joins.length
    ? node.joins.map(e => e.to('pg', options)).join(', ')
    : ''

  const where = node.where.length ?
    `WHERE ${node.where.map(e => wrapSQL(e.to('pg', options))).join(' AND ')}`
    : ''

  const having = node.having.length
    ? `HAVING ${node.having.map(e => wrapSQL(e.to('pg', options))).join(' AND ')}`
    : ''

  const groupBy = node.groupBy.length
    ? `GROUP BY ${node.groupBy.map(e => e.to('pg', options)).join(', ')}`
    : ''

  const orderBy = node.orderBy.length
    ? `ORDER BY ${node.orderBy.map(e => e.to('pg', options)).join(', ')}`
    : ''

  const limit = node.limit !== undefined ? `LIMIT ${node.limit}` : ''
  const offset = node.offset !== undefined ? `OFFSET ${node.offset}` : ''

  return `
    ${ctes}
    SELECT ${distinct}
      ${columns}
    ${from}
    ${joins}
    ${where}
    ${groupBy}
    ${having}
    ${orderBy}
    ${limit}
    ${offset}
  `
}

SelectNode.registerParser('pg', withOptions(selectParser, { alias: false }))

CTESelectNode.registerParser('pg', withOptions(
  (node, options) => `${escapeIdentifier(node.as)} AS (${selectParser(node, options)})`,
  { alias: false, cast: false },
))

RangeSelectNode.registerParser('pg', withOptions(selectParser, { cast: false }))

ShortNode.registerParser('pg', withOptions((node, options) => node.value.to('pg', options)))

SortNode.registerParser('pg', withOptions((node, options) => {
  const direction = node.direction ? ` ${node.direction}` : ''
  const nulls = node.nulls ? ` NULLS ${node.nulls}` : ''
  return wrapSQL(node.value.to('pg', options)) + direction + nulls
}))

SQLNode.registerParser('pg', withOptions((node, options) => node.value.to('pg', options)))

ViewReferenceNode.registerParser('pg', withOptions(node => escapeIdentifier(node.view)))

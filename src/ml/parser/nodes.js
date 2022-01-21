/* Node parsers */
const { useAPIErrorOptions } = require('../../util/api-error')
const functions = require('./functions')
const { wrapSQL, escapeIdentifier, escapeLiteral, trimSQL, ParserError } = require('./src/utils')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const withOptions = (parser, { alias = true, cast = true, trim = true } = {}) =>
  (node, options) => {
    try {
      let sql = parser(node, options)
      if (cast && node.cast) {
        sql = `CAST(${wrapSQL(sql)} AS ${node.cast})`
      }
      if (alias && node.as) {
        sql = `${wrapSQL(sql)} AS ${escapeIdentifier(node.as)}`
      }
      if (trim) {
        sql = trimSQL(sql)
      }
      return sql
    } catch (err) {
      if (err instanceof ParserError) {
        throw apiError(err.message, 400)
      }
      throw err
    }
  }

const arrayParser = engine => withOptions((node, options) =>
  `ARRAY[${node.values.map(e => e.to(engine, options)).join(', ')}]`)

const caseParser = engine => withOptions((node, options) => `
  CASE
    ${node.case
    .map(([cond, res]) =>
      `WHEN ${wrapSQL(cond.to(engine, options))} THEN ${wrapSQL(res.to(engine, options))}`)
    .join('\n')}
    ${node.defaultRes ? `ELSE ${wrapSQL(node.defaultRes.to(engine, options))}` : ''}
  END
`)

const castParser = engine => withOptions((node, options) => node.value.to(engine, options))

const columnParser = withOptions((node) => {
  const column = node.column === '*' ? '*' : escapeIdentifier(node.column)
  return node.view ? `${escapeIdentifier(node.view)}.${column}` : column
})

const functionParser = engine => withOptions((node, options) => {
  let name = { node }
  // function name is different or implementation is non-standard
  if (node.name in functions && engine in functions[node.name]) {
    if (typeof functions[node.name][engine] === 'function') {
      return functions[node.name][engine](node, options)
    }
    name = functions[node.name][engine]
  }
  return `${name}(${node.args.map(e => e.to(engine, options)).join(', ')})`
})

const geometryParser = engine => withOptions((node, options) => {
  const args = node.args.map(e => `UPPER(${wrapSQL(e.to(engine, options))})`).join(" || ':' || ")
  return `'geo:${node.type}:' || ${args}`
})

const joinParser = engine => withOptions(node =>
  `${node.joinType} JOIN ${node.view.to(engine)} ON ${wrapSQL(node.on.to(engine))}`)

const listParser = engine => withOptions((node, options) =>
  `(${node.values.map(e => e.to(engine, options)).join(', ')})`)

const operatorParser = engine => withOptions((node, options) => {
  if (node.name === 'between' || node.name === 'not between') {
    const [oA, oB, oC] = node.operands
    return `
      ${oA.to(engine, options)} ${node.name}
        ${oB.to(engine, options)} AND ${oC.to(engine, options)}
    `
  }
  return node.operands.map((o, i, all) => {
    const op = i > 0 || all.length === 1 ? `${node.name} ` : ''
    return op + o.to(engine, options)
  }).join(' ')
})

const parameterParser = engine => withOptions((node) => {
  if (node.value === undefined) {
    throw apiError('Missing parameter value', 400)
  }
  return node.value.to(engine)
})

const primitiveParser = withOptions(node =>
  (typeof node.value === 'string' ? escapeLiteral(node.value) : String(node.value)))


const baseSelectParser = engine => (node, options) => {
  const ctes = node.with.length
    ? `WITH ${node.with.map(e => e.to(engine, options)).join(', ')}`
    : ''

  const distinct = node.distinct ? 'DISTINCT' : ''
  const columns = node.columns
    .map(e => (e.as ? e.to(engine, options) : wrapSQL(e.to(engine, options))))
    .join(', ')

  const from = node.from ? `FROM ${node.from.to(engine, options)}` : ''

  const joins = node.joins.length
    ? node.joins.map(e => e.to(engine, options)).join(', ')
    : ''

  const where = node.where.length ?
    `WHERE ${node.where.map(e => wrapSQL(e.to(engine, options))).join(' AND ')}`
    : ''

  const having = node.having.length
    ? `HAVING ${node.having.map(e => wrapSQL(e.to(engine, options))).join(' AND ')}`
    : ''

  const groupBy = node.groupBy.length
    ? `GROUP BY ${node.groupBy.map(e => e.to(engine, options)).join(', ')}`
    : ''

  const orderBy = node.orderBy.length
    ? `ORDER BY ${node.orderBy.map(e => e.to(engine, options)).join(', ')}`
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

const selectParser = engine => withOptions(baseSelectParser(engine), { alias: false })

const cteSelectParser = (engine) => {
  const parser = baseSelectParser(engine)
  return withOptions(
    (node, options) => `${escapeIdentifier(node.as)} AS (${parser(node, options)})`,
    { alias: false, cast: false },
  )
}
const rangeSelectParser = engine => withOptions(baseSelectParser(engine), { cast: false })

const shortParser = engine => withOptions((node, options) => node.value.to(engine, options))

const sortParser = engine => withOptions((node, options) => {
  const direction = node.direction ? ` ${node.direction}` : ''
  const nulls = node.nulls ? ` NULLS ${node.nulls}` : ''
  return wrapSQL(node.value.to(engine, options)) + direction + nulls
})

const sqlParser = engine => withOptions((node, options) => node.value.to(engine, options))

const viewParser = withOptions(node => escapeIdentifier(node.view))

module.exports = {
  arrayParser,
  caseParser,
  castParser,
  columnParser,
  functionParser,
  geometryParser,
  joinParser,
  listParser,
  operatorParser,
  parameterParser,
  primitiveParser,
  selectParser,
  cteSelectParser,
  rangeSelectParser,
  shortParser,
  sortParser,
  sqlParser,
  viewParser,
}

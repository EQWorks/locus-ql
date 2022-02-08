/* Node parsers */
const { useAPIErrorOptions } = require('../../util/api-error')
const functions = require('./functions')
const operators = require('./operators')
const { escapeIdentifier, escapeLiteral, trimSQL, ParserError } = require('./src/utils')
const castMapping = require('./cast')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql', module: 'parser' } })

const withOptions = (parser, { alias = true, cast = true, trim = true, engine } = {}) =>
  (node, options) => {
    try {
      let sql = parser(node, options)
      if (cast && node.cast) {
        sql = `CAST(${sql} AS ${castMapping[engine][node.cast]})`
      }
      if (alias && node.as) {
        sql = `${sql} AS ${escapeIdentifier(node.as)}`
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
  `ARRAY[${node.values.map(e => e.to(engine, options)).join(', ')}]`, { engine })

const caseParser = engine => withOptions((node, options) => `
  CASE
    ${node.cases
    .map(([cond, res]) =>
      `WHEN ${cond.to(engine, options)} THEN ${res.to(engine, options)}`)
    .join('\n')}
    ${node.defaultRes ? `ELSE ${node.defaultRes.to(engine, options)}` : ''}
  END
`, { engine })

const castParser = engine => withOptions((node, options) =>
  node.value.to(engine, options), { engine })

const columnParser = engine => withOptions((node) => {
  const column = node.column === '*' ? '*' : escapeIdentifier(node.column)
  return node.view ? `${escapeIdentifier(node.view)}.${column}` : column
}, { engine })

const functionParser = engine => withOptions((node, options) => {
  let { name } = node
  let sql
  // function name is different or implementation is non-standard
  if (name in functions && engine in functions[name]) {
    if (typeof functions[name][engine] === 'function') {
      sql = functions[name][engine](node, options)
    }
    name = functions[name][engine]
  }
  if (!sql) {
    sql = `${name}(${node.args.map(e => e.to(engine, options)).join(', ')})`
    // return as subquery to hide underlying implementation (column name)
    if (name !== node.name && !node.as) {
      sql = `(SELECT ${sql} AS ${name})`
    }
  }
  const cast = node.cast || node.defaultCast
  return cast ? `CAST(${sql} AS ${castMapping[engine][cast]})` : sql
}, { engine, cast: false })

const geometryParser = engine => withOptions((node, options) => {
  const args = node.args.map(e => `UPPER(${e.to(engine, options)})`).join(" || ':' || ")
  return `'geo:${node.type}:' || ${args}`
}, { engine })

const joinParser = engine => withOptions((node, options) => {
  const view = node.view.to(engine, options)
  if (node.joinType === 'lateral') {
    return `CROSS JOIN LATERAL ${view}`
  }
  const joinType = node.joinType.toUpperCase()
  const on = node.on !== undefined ? `ON ${node.on.to(engine, options)}` : ''
  return `${joinType} JOIN ${view}${on}`
}, { engine })

const listParser = engine => withOptions((node, options) =>
  `(${node.values.map(e => e.to(engine, options)).join(', ')})`, { engine })

const operatorParser = engine => withOptions((node, options) => {
  let { qualifier, name } = node
  let sql
  if (name in operators && engine in operators[name]) {
    if (typeof operators[name][engine] === 'function') {
      sql = operators[name][engine](node, options)
    } else {
      name = operators[name][engine]
    }
  }
  if (!sql) {
    if (qualifier in operators && engine in operators[qualifier]) {
      // if qualifier has non-standard implementation throw error
      if (typeof operators[qualifier][engine] === 'function') {
        throw apiError(`Invalid operator qualifier for operator: ${qualifier}`)
      }
      qualifier = operators[name][engine]
    }
    const operator = `${qualifier ? `${qualifier} ` : ''}${name} `
    sql = node.operands.map((o, i, all) => {
      const op = i > 0 || all.length === 1 ? operator : ''
      return op + o.to(engine, options)
    }).join(' ')
  }
  return node.isRoot() && !node.as && !node.cast ? sql : `(${sql})`
}, { engine })

const parameterParser = engine => withOptions((node, options) => {
  if (node.value === undefined) {
    throw apiError('Missing parameter value', 400)
  }
  return node.value.to(engine, options)
}, { engine })

const primitiveParser = engine => withOptions((node) => {
  if (typeof node.value === 'string') {
    // trino has no escape character
    return engine === 'trino' ? node.value.replace(/'/g, "''") : escapeLiteral(node.value)
  }
  return String(node.value)
}, { engine })


const baseSelectParser = engine => (node, options) => {
  const ctes = node.with.length
    ? `WITH ${node.with.map(e => e.to(engine, options)).join(', ')}`
    : ''

  const distinct = node.distinct ? 'DISTINCT' : ''
  const columns = node.columns.map(e => e.to(engine, options)).join(', ')

  const from = node.from ? `FROM ${node.from.to(engine, options)}` : ''

  const joins = node.joins.length
    ? node.joins.map(e => e.to(engine, options)).join(' ')
    : ''

  const where = node.where.length ?
    `WHERE ${node.where.map(e => e.to(engine, options)).join(' AND ')}`
    : ''

  const having = node.having.length
    ? `HAVING ${node.having.map(e => e.to(engine, options)).join(' AND ')}`
    : ''

  const groupBy = node.groupBy.length
    ? `GROUP BY ${node.groupBy.map(e => e.to(engine, options)).join(', ')}`
    : ''

  const orderBy = node.orderBy.length
    ? `ORDER BY ${node.orderBy.map(e => e.to(engine, options)).join(', ')}`
    : ''

  const limit = node.limit !== undefined ? `LIMIT ${node.limit}` : ''
  const offset = node.offset !== undefined ? `OFFSET ${node.offset}` : ''

  const sql = `
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
  return node.isRoot() && !node.as && !node.cast ? sql : `(${sql})`
}

const selectParser = engine => withOptions(baseSelectParser(engine), { engine })

const cteSelectParser = (engine) => {
  const parser = baseSelectParser(engine)
  return withOptions(
    (node, options) => `${escapeIdentifier(node.as)} AS ${parser(node, options)}`,
    { alias: false, cast: false, engine },
  )
}
const rangeSelectParser = engine => withOptions(baseSelectParser(engine), { cast: false, engine })

const shortParser = engine => withOptions((node, options) =>
  node.value.to(engine, options), { engine })

const sortParser = engine => withOptions((node, options) => {
  const direction = node.direction ? ` ${node.direction}` : ''
  const nulls = node.nulls ? ` NULLS ${node.nulls}` : ''
  return node.value.to(engine, options) + direction + nulls
}, { engine })

const sqlParser = engine => withOptions((node, options) =>
  node.value.to(engine, options), { engine })

const viewParser = engine => withOptions(node => escapeIdentifier(node.view), { engine })

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

/* eslint-disable no-continue */
/* eslint-disable no-use-before-define */
const sqlParser = require('pgsql-parser')

const {
  // isArray,
  isString,
  isNonArrayObject,
  isObjectExpression,
  expressionTypes: expTypes,
  parserError,
  extractShortExpressionsFromSQL,
} = require('./utils')


const findApproxLocation = (exp) => {
  // look for location in current exp (ast node)
  // if not visit every node (depth first) and look for location
  if (typeof exp !== 'object' || exp === null) {
    return
  }
  if ('location' in exp && exp.location !== -1) {
    return exp.location
  }
  for (const node of Object.values(exp)) {
    const location = findApproxLocation(node)
    if (location) {
      return location
    }
  }
}

class SQLParserError extends Error {
  constructor({ location, message = 'Syntax not supported', expression }) {
    super(message)
    this.location = location !== undefined ? location : findApproxLocation(expression)
  }
}
const sqlParserError = args => new SQLParserError(args)

// const parseShortArgs = args => args.map((a) => {
//   // primitive or short
//   if (typeof a !== 'object' || a === null) {
//     return typeof a !== 'string' || a.startsWith('@') ? a : `'${a}'`
//   }
//   // array
//   if (isArray(a)) {
//     return `[${parseShortArgs(a)}]`
//   }
//   // json
//   if (a.type === 'primitive' && ['json', 'jsonb'].includes(a.cast)) {
//     return a.value
//   }
//   // named arg
//   if (
//     a.type === 'operator'
//     && a.values[0] === '='
//     && isObjectExpression(a.values[1], expTypes.COLUMN)
//   ) {
//     const name = a.values[1].column
//     const value = parseShortArgs([a.values[2]])
//     return `${name}=${value}`
//   }
//   throw parserError('Invalid argument in short expression')
// }).join(',')

const astParsers = {}
astParsers.String = ({ str }) => str
astParsers.Integer = ({ ival }) => ival
astParsers.Float = ({ str }) => Number(str)
astParsers.Null = () => null
astParsers.RangeVar = ({ relname: view, alias: { aliasname: as } = {} }) =>
  ({ type: expTypes.VIEW, view, as })
astParsers.RangeSubselect = ({ subquery, alias: { aliasname: as } = {} }, context) =>
  Object.assign(parseASTNode(subquery, context), { as })
astParsers.SubLink = ({ testexpr, subselect, subLinkType, operName, location }, context) => {
  // subLinkType: EXISTS_SUBLINK, ALL_SUBLINK, ANY_SUBLINK, ROWCOMPARE_SUBLINK,
  // EXPR_SUBLINK, ARRAY_SUBLINK, CTE_SUBLINK
  const type = subLinkType.slice(0, -8).toLowerCase()
  const operator = operName ? parseASTNode(operName[0], context) : undefined
  const value = testexpr ? parseASTNode(testexpr, context) : undefined
  const subquery = parseASTNode(subselect, context)
  if (type === 'expr') {
    return subquery
  }
  if (type === 'exists') {
    return { type: expTypes.FUNCTION, values: [type, subquery] }
  }
  if (['any', 'all'].includes(type)) {
    return {
      type: expTypes.OPERATOR,
      values: [operator || '=', value, { type: expTypes.FUNCTION, values: [type, subquery] }],
    }
  }
  throw sqlParserError({ message: 'Subselect query not supported', location })
}
// top level
astParsers.RawStmt = ({ stmt }, context) => parseASTNode(stmt, context)

astParsers.SelectStmt = (exp, context) => {
  const {
    distinctClause,
    targetList,
    fromClause,
    whereClause,
    groupClause,
    havingClause,
    sortClause,
    // limitOption,
    limitCount,
    limitOffset,
    withClause: { ctes: cteClause } = {},
    valuesLists,
  } = exp
  if (valuesLists) {
    throw sqlParserError({
      message: 'Lists of values not supported',
      expression: valuesLists,
    })
  }

  // WITH
  const ctes = cteClause ? cteClause.map(e => parseASTNode(e, context)) : undefined

  // FROM AND JOINS
  let from // should be string (view or alias) or object (view and/or alias)
  let joins
  if (fromClause) {
    const queue = [parseASTNode(fromClause[0], context)]
    joins = []
    while (queue.length) {
      const item = queue.shift()
      if (item.type !== expTypes.JOIN) {
        from = item
        continue
      }
      const { type, joinType, on, right: view, left } = item
      joins.push({ type, joinType, on, view })
      if (left.type === expTypes.JOIN) {
        queue.push(left)
        continue
      }
      from = left
    }
    joins = joins.length ? joins.reverse() : undefined
  }

  // DISTINCT AND COLUMNS
  const distinct = distinctClause !== undefined || undefined
  const columns = targetList.map(e => parseASTNode(e, context))

  // WHERE
  let where
  if (whereClause) {
    const value = parseASTNode(whereClause, context)
    if (isObjectExpression(value, 'operator') && value.values[0] === 'and') {
      where = value.values.slice(1)
    } else {
      where = [value]
    }
  }

  // HAVING
  let having
  if (havingClause) {
    const value = parseASTNode(havingClause, context)
    if (isObjectExpression(value, 'operator') && value.values[0] === 'and') {
      having = value.values.slice(1)
    } else {
      having = [value]
    }
  }

  // GROUP BY AND ORDER BY
  const groupBy = groupClause ? groupClause.map(e => parseASTNode(e, context)) : undefined
  const orderBy = sortClause ? sortClause.map(e => parseASTNode(e, context)) : undefined

  // LIMIT
  // if (limitOption !== 'LIMIT_OPTION_DEFAULT') {
  const limit = limitCount ? parseASTNode(limitCount, context) : undefined
  // }

  // OFFSET
  const offset = limitOffset ? parseASTNode(limitOffset, context) : undefined

  return {
    type: expTypes.SELECT,
    distinct,
    with: ctes,
    from,
    joins,
    columns,
    where,
    having,
    groupBy,
    orderBy,
    limit,
    offset,
  }
}

astParsers.ResTarget = ({ name: as, val }, context) => {
  const value = parseASTNode(val, context)
  if (!as) {
    return value
  }
  if (isNonArrayObject(value)) {
    return Object.assign(value, { as })
  }
  return { type: expTypes.PRIMITIVE, value, as }
}

astParsers.JoinExpr = ({ jointype, larg, rarg, quals }, context) => ({
  type: expTypes.JOIN,
  joinType: jointype.slice(5).toLowerCase(),
  on: parseASTNode(quals, context),
  left: parseASTNode(larg, context),
  right: parseASTNode(rarg, context),
})

astParsers.CommonTableExpr = ({ ctename: as, ctequery }, context) =>
  Object.assign(parseASTNode(ctequery, context), { as })

astParsers.SortBy = (exp, context) => {
  const { node, sortby_dir, sortby_nulls } = exp
  const value = parseASTNode(node, context)
  let direction = sortby_dir.slice(7).toLowerCase()
  if (direction === 'using') {
    throw sqlParserError({
      message: '"using" is not supported in order by clause',
      expression: exp,
    })
  }
  direction = direction !== 'default' ? direction : undefined
  let nulls = sortby_nulls.slice(13).toLowerCase()
  nulls = nulls !== 'default' ? nulls : undefined
  return { type: expTypes.SORT, value, direction, nulls }
}

astParsers.CaseExpr = ({ args, defresult }, context) => {
  const defaultRes = defresult ? parseASTNode(defresult, context) : undefined
  const cases = args.map(e => parseASTNode(e, context))
  return {
    type: expTypes.CASE,
    values: defaultRes ? [defaultRes, ...cases] : cases,
  }
}

astParsers.CaseWhen = ({ expr, result }, context) =>
  [parseASTNode(expr, context), parseASTNode(result, context)]

astParsers.FuncCall = ({ funcname, args, over, location }, context) => {
  if (over) {
    throw sqlParserError({ message: 'Window function not supported', location })
  }
  const name = parseASTNode(funcname[0], context).toLowerCase()
  const parsedArgs = args ? args.map(e => parseASTNode(e, context)) : []
  return { type: expTypes.FUNCTION, values: [name, ...parsedArgs] }
}

astParsers.CoalesceExpr = ({ args }, context) => ({
  type: expTypes.FUNCTION,
  values: ['coalesce', ...args.map(e => parseASTNode(e, context))],
})

astParsers.TypeCast = (exp, context) => {
  const { arg, typeName } = exp
  const value = parseASTNode(arg, context)
  const cast = parseASTNode(typeName.names.slice(-1)[0], context).toLowerCase()
  // boolean
  if (cast === 'bool' && ['t', 'f'].includes(value)) {
    return value === 't'
  }
  // interval - 'int quantity unit(s)'
  if (cast === 'interval' && isString(value)) {
    const safeValue = value.toLowerCase()
    if (!/^(\d+\s(millisecond|second|minute|hour|day|week|month|year)s?(\s(?!$)|$))+/
      .test(safeValue)) {
      throw sqlParserError({ message: 'interval syntax not supported', expression: exp })
    }
    const intervals = safeValue.split(' ').reduce((acc, v, i) => {
      if (i % 2) {
        // unit
        acc[acc.length - 1].value += ` ${v[v.length - 1] === 's' ? v.slice(0, -1) : v}`
      } else {
        // quantity
        acc.push({
          type: expTypes.PRIMITIVE,
          value: v,
          cast,
        })
      }
      return acc
    }, ['+'])
    if (intervals.length === 2) {
      return intervals[1]
    }
    return {
      type: expTypes.OPERATOR,
      values: intervals,
    }
  }
  if (!isObjectExpression(value)) {
    return { type: expTypes.PRIMITIVE, value, cast }
  }
  if (!('cast' in value)) {
    return Object.assign(value, { cast })
  }
  return { type: expTypes.CAST, value, cast }
}

astParsers.ColumnRef = ({ fields }, context) => {
  let view
  let column
  if (fields.length === 1) {
    column = parseASTNode(fields[0], context)
  } else {
    view = parseASTNode(fields[0], context)
    column = parseASTNode(fields[1], context)
  }
  return { type: expTypes.COLUMN, view, column }
}

// used for shorts
astParsers.ParamRef = ({ number, location }, context) => {
  // shorts does not exist i.e. param ref came from user
  if (number > context.shorts.length) {
    throw sqlParserError({ message: 'Invalid parameter reference or short expression', location })
  }
  return { type: expTypes.SHORT, value: context.shorts[number - 1] }
}

astParsers.ListType = ({ items }, context) =>
  ({ type: expTypes.LIST, values: items.map(e => parseASTNode(e, context)) })

astParsers.A_ArrayExpr = ({ elements }, context) =>
  ({ type: expTypes.ARRAY, values: elements.map(e => parseASTNode(e, context)) })

astParsers.A_Const = ({ val }, context) => parseASTNode(val, context)
astParsers.A_Star = () => '*'
astParsers.A_Expr = ({ kind, name, lexpr, rexpr, location }, context) => {
  // kind: AEXPR_OP, AEXPR_AND, AEXPR_OR, AEXPR_NOT,
  // AEXPR_OP_ANY, AEXPR_OP_ALL, AEXPR_DISTINCT, AEXPR_NULLIF,
  // AEXPR_OF, AEXPR_IN
  const safeKind = kind.slice(6).toLowerCase()
  let operator = parseASTNode(name[0], context).toLowerCase()
  const left = lexpr ? parseASTNode(lexpr, context) : undefined
  let right = parseASTNode(rexpr, context)

  switch (safeKind) {
    case 'op':
      // // short expression
      // if (
      //   operator === '@'
      //   && isObjectExpression(right, expTypes.FUNCTION)
      //   && right.values[0].startsWith('_')
      // ) {
      //   const name = right.values[0].slice(1)
      //   return `@${name}(${parseShortArgs(right.values.slice(1))})`
      // }
      break

    case 'nullif':
      return { type: expTypes.FUNCTION, values: [safeKind, left, right] }

    case 'and':
    case 'or':
    case 'not':
      operator = safeKind
      break

    case 'op_any':
    case 'op_all':
      // left operator kind (right)
      right = { type: expTypes.FUNCTION, values: [safeKind.slice(3), right] }
      break

    case 'of':
      // left IS [operator - NOT] OF right
      operator = `is${operator === '<>' ? ' not' : ''} of`
      break

    case 'in':
      // left [operator - NOT] in right
      operator = `${operator === '<>' ? 'not ' : ''}in`
      break

    case 'distinct':
      // left is distinct from right
      operator = 'is distinct from'
      break

    default:
      throw sqlParserError({ message: 'Operation not supported', location })
  }

  return {
    type: expTypes.OPERATOR,
    values: left !== undefined ? [operator, left, right] : [operator, right],
  }
}

astParsers.BoolExpr = ({ boolop, args }, context) => ({
  type: expTypes.OPERATOR,
  values: [boolop.slice(0, -5).toLowerCase(), ...args.map(e => parseASTNode(e, context))],
})

astParsers.NullTest = ({ arg, nulltesttype }, context) => {
  const operator = nulltesttype === 'IS_NOT_NULL' ? 'IS NOT' : 'IS'
  return { type: expTypes.OPERATOR, values: [operator, parseASTNode(arg, context), null] }
}

const parseASTNode = (node, context = { shorts: [] }) => {
  const type = Object.keys(node)[0] // ast node type
  const exp = node[type]
  if (!(type in astParsers)) {
    throw sqlParserError({ expression: exp })
  }
  return astParsers[type](exp, context)
}

const parseSQLToAST = (sql) => {
  let originalError
  // try to parse sql as is
  try {
    const [ast] = sqlParser.parse(sql)
    return ast
  } catch (err) {
    originalError = err
  }
  // on failure, nest into select statement
  try {
    const [ast] = sqlParser.parse(`SELECT ${sql}`)
    return ast.RawStmt.stmt.SelectStmt.targetList[0].ResTarget.val
  } catch (_) {
    throw parserError(`SQL error: ${originalError.message}`)
  }
}

const isValidSQLExpression = (sql) => {
  try {
    const sqlWithoutShorts = extractShortExpressionsFromSQL(sql).sql
    parseSQLToAST(sqlWithoutShorts)
    return true
  } catch (_) {
    return false
  }
}

const parseSQLExpression = (sql) => {
  const { sql: sqlWithoutShorts, shorts } = extractShortExpressionsFromSQL(sql)
  const ast = parseSQLToAST(sqlWithoutShorts)
  try {
    return parseASTNode(ast, { shorts })
  } catch (err) {
    if (err instanceof SQLParserError) {
      const excerpt = err.location !== undefined
        ? ` at or near: "${sql.slice(err.location, err.location + 30)}..."`
        : ''
      throw parserError(err.message + excerpt)
    }
    throw err
  }
}

module.exports = {
  parseSQLExpression,
  isValidSQLExpression,
}

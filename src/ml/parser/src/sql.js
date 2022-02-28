/* eslint-disable no-continue */
/* eslint-disable no-use-before-define */
const sqlParser = require('pgsql-parser')

const {
  // isArray,
  isString,
  isNonArrayObject,
  isObjectExpression,
  parserError,
  extractShortExpressionsFromSQL,
} = require('./utils')
const { expressionTypes: expTypes } = require('./types')


const intervalLiteralRE =
  /^\s*(\d+\s+(millisecond|second|minute|hour|day|week|month|year)s?(\s+(?!$)|\s*$))+/
const geometryLiteralRE = /^\s*[-a-z]+\s+[(]([-.a-z0-9]+(\s+(?![)])|\s*[)]))+\s*$/

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
astParsers.RangeSubselect = ({ subquery, lateral, alias: { aliasname: as } = {} }, context) => {
  const select = parseASTNode(subquery, context)
  if (lateral) {
    select.type = expTypes.SELECT_RANGE_LATERAL
  }
  return Object.assign(select, { as })
}

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
    limitOption, // LIMIT_OPTION_DEFAULT, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES
    limitCount,
    limitOffset,
    withClause,
    valuesLists,
    op, // SETOP_NONE, SETOP_UNION, SETOP_INTERSECT, SETOP_EXCEPT
    all,
    larg,
    rarg,
    lockingClause,
  } = exp

  if (valuesLists) {
    throw sqlParserError({ message: 'Lists of values not supported', expression: valuesLists })
  }

  if (lockingClause) {
    throw sqlParserError({ message: 'Locking not supported', expression: lockingClause })
  }

  // OPERATOR
  const safeOp = op.slice(6).toLowerCase() // none, union, intersect or except
  let operator
  let distinct
  let operands
  if (safeOp !== 'none') {
    operator = safeOp
    distinct = !all
    operands = [larg, rarg].map(SelectStmt => parseASTNode({ SelectStmt }, context))
  }

  // WITH
  let ctes
  if (withClause) {
    if (withClause.recursive) {
      throw sqlParserError({ message: '"with recursive" not supported', expression: withClause })
    }
    ctes = withClause.ctes.map(e => parseASTNode(e, context))
  }

  // FROM AND JOINS
  let from // should be string (view or alias) or object (view and/or alias)
  let joins
  if (fromClause) {
    const queue = fromClause.map(e => parseASTNode(e, context))
    joins = []
    while (queue.length) {
      const item = queue.shift()
      if (item.type !== expTypes.JOIN) {
        if (!from) {
          from = item
          continue
        }
        joins.push({ type: expTypes.JOIN, joinType: 'cross', view: item })
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

  // DISTINCT
  if (distinctClause) {
    if (distinctClause.length > 1 || Object.keys(distinctClause[0]).length) {
      throw sqlParserError({ message: '"distinct on" not supported', expression: distinctClause })
    }
    distinct = true
  }

  // COLUMNS
  const columns = targetList ? targetList.map(e => parseASTNode(e, context)) : undefined

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
  if (limitOption === 'LIMIT_OPTION_WITH_TIES') {
    throw sqlParserError({ message: 'Fetch with ties not supported', expression: limitOffset })
  }
  const limit = limitCount ? parseASTNode(limitCount, context) : undefined

  // OFFSET
  const offset = limitOffset ? parseASTNode(limitOffset, context) : undefined

  return {
    type: expTypes.SELECT,
    operator,
    operands,
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

astParsers.JoinExpr = (exp, context) => {
  const { jointype, larg, rarg, quals, usingClause } = exp
  if (usingClause) {
    throw sqlParserError({ message: '"using" not supported in join expression', expression: exp })
  }
  if (!['JOIN_INNER', 'JOIN_LEFT', 'JOIN_RIGHT'].includes(jointype)) {
    throw sqlParserError({ message: 'Join type not supported', expression: exp })
  }
  let joinType = jointype.slice(5).toLowerCase()
  const right = parseASTNode(rarg, context)
  if (joinType === 'inner' && !quals) {
    joinType = right.type === expTypes.SELECT_RANGE_LATERAL ? 'lateral' : 'cross'
  }
  return {
    type: expTypes.JOIN,
    joinType,
    on: quals ? parseASTNode(quals, context) : undefined,
    left: parseASTNode(larg, context),
    right,
  }
}

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
  if (cast === 'date') {
    return {
      type: expTypes.FUNCTION,
      values: ['date', value],
    }
  }
  if (cast === 'timestamp' || cast === 'timestamptz') {
    return {
      type: expTypes.FUNCTION,
      values: ['datetime', value],
    }
  }
  // interval - 'int quantity unit(s)'
  if (cast === 'interval' && isString(value, true)) {
    const safeValue = value.toLowerCase()
    if (!intervalLiteralRE.test(safeValue)) {
      throw sqlParserError({ message: 'Interval syntax not supported', expression: exp })
    }
    const intervals = safeValue.split(' ').filter(v => v !== '').reduce((acc, v, i) => {
      if (i % 2 === 0) {
        // quantity
        acc.push({
          type: expTypes.FUNCTION,
          values: ['timedelta', undefined, parseInt(v)],
        })
      } else {
        // unit
        acc[acc.length - 1].values[1] = `${v[v.length - 1] === 's' ? v.slice(0, -1) : v}`
      }
      return acc
    }, [])
    if (intervals.length === 1) {
      return intervals[0]
    }
    return {
      type: expTypes.OPERATOR,
      values: ['+', ...intervals],
    }
  }
  // geometry - '<type> (arg arg arg)'
  if (cast === 'geometry' && isString(value, true)) {
    const safeValue = value.toLowerCase()
    if (!geometryLiteralRE.test(safeValue)) {
      throw sqlParserError({ message: 'Geometry syntax not supported', expression: exp })
    }
    const [type, args] = safeValue.split('(')
    return {
      type: expTypes.FUNCTION,
      values: [
        'geometry',
        type.trim(),
        ...args.split(')')[0].toUpperCase().split(' ').filter(v => v !== ''),
      ],
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

astParsers.List = ({ items }, context) =>
  ({ type: expTypes.LIST, values: items.map(e => parseASTNode(e, context)) })

astParsers.A_ArrayExpr = ({ elements }, context) =>
  ({ type: expTypes.ARRAY, values: elements.map(e => parseASTNode(e, context)) })

astParsers.A_Const = ({ val }, context) => parseASTNode(val, context)
astParsers.A_Star = () => '*'
astParsers.A_Indices = (exp, context) => {
  const { is_slice, uidx } = exp
  if (is_slice) {
    throw sqlParserError({ message: 'Slice operation not supported', expression: exp })
  }
  return parseASTNode(uidx, context)
}
astParsers.A_Expr = ({ kind, name, lexpr, rexpr, location }, context) => {
  // kind: AEXPR_OP, AEXPR_AND, AEXPR_OR, AEXPR_NOT,
  // AEXPR_OP_ANY, AEXPR_OP_ALL, AEXPR_DISTINCT, AEXPR_NULLIF,
  // AEXPR_OF, AEXPR_IN
  const safeKind = kind.slice(6).toLowerCase()
  let operator = parseASTNode(name[0], context).toLowerCase()
  const left = lexpr ? parseASTNode(lexpr, context) : undefined
  const right = parseASTNode(rexpr, context)

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
      operator = [operator, safeKind.slice(3)]
      break

    case 'of':
      // left IS [operator - NOT] OF right
      operator = `is${operator === '<>' ? ' not' : ''} of`
      break

    case 'in':
      // left [operator - NOT] in right
      operator = operator === '<>' ? ['not', 'in'] : 'in'
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

astParsers.A_Indirection = (exp, context) => {
  const { arg, indirection } = exp
  return {
    type: expTypes.OPERATOR,
    values: ['[]', parseASTNode(arg, context), ...indirection.map(e => parseASTNode(e, context))],
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
    if (/^syntax error at or near "\$\d+"$/.test(originalError.message)) {
      throw parserError('Invalid short expression or parameter reference')
    }
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

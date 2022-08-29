/* eslint-disable no-continue */
/* eslint-disable no-use-before-define */
const sqlParser = require('pgsql-parser')

const {
  isString,
  isNonArrayObject,
  isObjectExpression,
  parserError,
  extractShortExpressionsFromSQL,
} = require('./utils')
const { expressionTypes: expTypes, castTypes } = require('./types')


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

// throws on unhandled props
const checkProps = (handledProps, cb) => {
  const props = new Set(handledProps)
  props.add('location')
  return (expression, context) => {
    if (Object.keys(expression).some(p => !props.has(p))) {
      console.log(
        '** Unsupported sytax **\nUnknown props:\n',
        Object.keys(expression).filter(p => !props.has(p)),
        '\nEXpression:\n',
        expression,
      )
      throw sqlParserError({ expression, location: expression.location })
    }
    return cb(expression, context)
  }
}

class SQLParserError extends Error {
  constructor({ location, message = 'Syntax not supported', expression }) {
    super(message)
    this.location = location !== undefined ? location : findApproxLocation(expression)
  }
}
// factory function
const sqlParserError = args => new SQLParserError(args)

const astParsers = {}
astParsers.String = checkProps(['str'], ({ str }) => str)
astParsers.Integer = checkProps(['ival'], ({ ival }) => ival)
astParsers.Float = checkProps(['str'], ({ str }) => Number(str))
astParsers.Null = checkProps([], () => null)
astParsers.RangeVar = checkProps(
  ['relname', 'alias', 'inh', 'relpersistence'],
  ({ relname: view, alias: { aliasname: as } = {} }) => ({ type: expTypes.VIEW, view, as }),
)
astParsers.RangeSubselect = checkProps(
  ['subquery', 'lateral', 'alias'],
  ({ subquery, lateral, alias: { aliasname: as } = {} }, context) => {
    const select = parseASTNode(subquery, context)
    if (lateral) {
      select.type = expTypes.SELECT_RANGE_LATERAL
    }
    return Object.assign(select, { as })
  },
)

astParsers.SubLink = checkProps(
  ['testexpr', 'subselect', 'subLinkType', 'operName'],
  ({ testexpr, subselect, subLinkType, operName, location }, context) => {
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
      return { type: expTypes.OPERATOR, values: [type, subquery] }
    }
    if (['any', 'all'].includes(type)) {
      return {
        type: expTypes.OPERATOR,
        values: [[operator || '=', type], value, subquery],
      }
    }
    throw sqlParserError({ message: 'Subselect query not supported', location })
  },
)
// top level
astParsers.RawStmt = checkProps(
  ['stmt', 'stmt_len'],
  ({ stmt }, context) => parseASTNode(stmt, context),
)

astParsers.SelectStmt = checkProps([
  'distinctClause',
  'targetList',
  'fromClause',
  'whereClause',
  'groupClause',
  'havingClause',
  'sortClause',
  'limitOption',
  'limitCount',
  'limitOffset',
  'withClause',
  'valuesLists',
  'op',
  'all',
  'larg',
  'rarg',
  'lockingClause',

], (exp, context) => {
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
})

astParsers.ResTarget = checkProps(['name', 'val'], ({ name: as, val }, context) => {
  const value = parseASTNode(val, context)
  if (!as) {
    return value
  }
  if (isNonArrayObject(value)) {
    return Object.assign(value, { as })
  }
  return { type: expTypes.PRIMITIVE, value, as }
})

astParsers.JoinExpr = checkProps(
  ['jointype', 'larg', 'rarg', 'quals', 'usingClause'],
  (exp, context) => {
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
  },
)

astParsers.CommonTableExpr = checkProps(
  ['ctename', 'ctequery', 'ctematerialized'],
  ({ ctename: as, ctequery }, context) =>
    Object.assign(parseASTNode(ctequery, context), { as }),
)

astParsers.SortBy = checkProps(['node', 'sortby_dir', 'sortby_nulls'], (exp, context) => {
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
})

astParsers.CaseExpr = checkProps(['args', 'defresult'], ({ args, defresult }, context) => {
  const defaultRes = defresult ? parseASTNode(defresult, context) : undefined
  const cases = args.map(e => parseASTNode(e, context))
  return {
    type: expTypes.CASE,
    values: defaultRes !== undefined ? [defaultRes, ...cases] : cases,
  }
})

astParsers.CaseWhen = checkProps(['expr', 'result'], ({ expr, result }, context) =>
  [parseASTNode(expr, context), parseASTNode(result, context)])

astParsers.FuncCall = checkProps(
  ['funcname', 'args', 'agg_distinct', 'agg_filter', 'agg_order', 'agg_within_group', 'over'],
  (
    {
      funcname, args, agg_distinct: distinct,
      agg_filter, agg_order, agg_within_group, over, location,
    },
    context,
  ) => {
    // Window function
    const overClauses = {}
    if (over) {
      overClauses.partitionBy = over.partitionClause ?
        over.partitionClause.map(e => parseASTNode(e, context)) : undefined
      overClauses.orderBy = over.orderClause ?
        over.orderClause.map(e => parseASTNode(e, context)) : undefined
    }
    // filter (where ...)
    let where
    if (agg_filter) {
      const value = parseASTNode(agg_filter, context)
      if (isObjectExpression(value, 'operator') && value.values[0] === 'and') {
        where = value.values.slice(1)
      } else {
        where = [value]
      }
    }
    // order by
    if (agg_within_group) {
      // within group (order by ...)
      throw sqlParserError({ message: 'Within group not supported', location })
    }
    const orderBy = agg_order ? agg_order.map(e => parseASTNode(e, context)) : undefined
    const name = parseASTNode(funcname[0], context).toLowerCase()
    const parsedArgs = args ? args.map(e => parseASTNode(e, context)) : []
    return {
      type: expTypes.FUNCTION,
      values: [name, ...parsedArgs],
      distinct,
      where,
      orderBy,
      over: overClauses,
    }
  },
)

astParsers.CoalesceExpr = checkProps(['args'], ({ args }, context) => ({
  type: expTypes.FUNCTION,
  values: ['coalesce', ...args.map(e => parseASTNode(e, context))],
}))

const sqlCastTypes = {
  // numeric/float
  numeric: castTypes.NUMERIC,
  real: castTypes.FLOAT,
  float4: castTypes.FLOAT,
  float8: castTypes.FLOAT,
  decimal: castTypes.FLOAT,
  double: castTypes.FLOAT,
  'double precision': castTypes.FLOAT,
  // integer
  integer: castTypes.INTEGER,
  int2: castTypes.INTEGER,
  smallint: castTypes.INTEGER,
  int4: castTypes.INTEGER,
  int: castTypes.INTEGER,
  int8: castTypes.INTEGER,
  bigint: castTypes.INTEGER,
  // string/text
  string: castTypes.STRING,
  text: castTypes.TEXT,
  varchar: castTypes.TEXT,
  'character varying': castTypes.TEXT,
  char: castTypes.TEXT,
  character: castTypes.TEXT,
  // bool
  boolean: castTypes.BOOLEAN,
  bool: castTypes.BOOLEAN,
  // json
  json: castTypes.JSON,
  jsonb: castTypes.JSON,
}

astParsers.TypeCast = checkProps(['arg', 'typeName'], (exp, context) => {
  const { arg, typeName } = exp
  const value = parseASTNode(arg, context)
  let cast = parseASTNode(typeName.names.slice(-1)[0], context).toLowerCase()
  // substitute with QL cast value
  if (cast in sqlCastTypes) {
    cast = sqlCastTypes[cast]
  }
  // boolean
  if (cast === castTypes.BOOLEAN && ['t', 'f'].includes(value)) {
    return value === 't'
  // date literal
  } else if (cast === 'date') {
    return {
      type: expTypes.FUNCTION,
      values: ['date', value],
    }
  // timestamp literal
  } else if (cast === 'timestamp' || cast === 'timestamptz') {
    return {
      type: expTypes.FUNCTION,
      values: ['datetime', value],
    }
  // interval literal
  // interval - 'int quantity unit(s)'
  } else if (cast === 'interval' && isString(value, true)) {
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
  // geometry literal
  // geometry - '<type> (arg arg arg)'
  } else if (cast === 'geometry' && isString(value, true)) {
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
})

astParsers.ColumnRef = checkProps(['fields'], ({ fields }, context) => {
  let view
  let column
  if (fields.length === 1) {
    column = parseASTNode(fields[0], context)
  } else {
    view = parseASTNode(fields[0], context)
    column = parseASTNode(fields[1], context)
  }
  return { type: expTypes.COLUMN, view, column }
})

// used for shorts
astParsers.ParamRef = checkProps(['number'], ({ number, location }, context) => {
  // shorts does not exist i.e. param ref came from user
  if (number > context.shorts.length) {
    throw sqlParserError({ message: 'Invalid parameter reference or short expression', location })
  }
  return { type: expTypes.SHORT, value: context.shorts[number - 1] }
})

astParsers.List = checkProps(['items'], ({ items }, context) =>
  ({ type: expTypes.LIST, values: items.map(e => parseASTNode(e, context)) }))

astParsers.A_ArrayExpr = checkProps(['elements'], ({ elements }, context) =>
  ({ type: expTypes.ARRAY, values: elements.map(e => parseASTNode(e, context)) }))

astParsers.A_Const = checkProps(['val'], ({ val }, context) => parseASTNode(val, context))
astParsers.A_Star = checkProps([], () => '*')
astParsers.A_Indices = checkProps(['is_slice', 'uidx'], (exp, context) => {
  const { is_slice, uidx } = exp
  if (is_slice) {
    throw sqlParserError({ message: 'Slice operation not supported', expression: exp })
  }
  return parseASTNode(uidx, context)
})
astParsers.A_Expr = checkProps(
  ['kind', 'name', 'lexpr', 'rexpr'],
  ({ kind, name, lexpr, rexpr, location }, context) => {
    // kind: AEXPR_OP, AEXPR_AND, AEXPR_OR, AEXPR_NOT,
    // AEXPR_OP_ANY, AEXPR_OP_ALL, AEXPR_DISTINCT, AEXPR_NULLIF,
    // AEXPR_OF, AEXPR_IN
    const safeKind = kind.slice(6).toLowerCase()
    let operator = parseASTNode(name[0], context).toLowerCase()
    const left = lexpr ? parseASTNode(lexpr, context) : undefined
    const right = parseASTNode(rexpr, context)

    switch (safeKind) {
      case 'op':
        break

      case 'nullif':
        return { type: expTypes.FUNCTION, values: [safeKind, left, right] }

      case 'and':
      case 'or':
      case 'not':
        operator = safeKind
        break

      case 'like':
        operator = operator === '!~~' ? ['not ', 'like'] : 'like'
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
      case 'not_distinct':
        // left is distinct from right
        operator = `is${safeKind === 'not_distinct' ? ' not' : ''} distinct from`
        break

      case 'between':
      case 'not_between':
        return {
          type: expTypes.OPERATOR,
          values: [
            safeKind === 'not_between' ? ['not', 'between'] : 'between',
            left,
            right.values[0],
            right.values[1],
          ],
        }

      default:
        throw sqlParserError({ message: 'Operation not supported', location })
    }

    return {
      type: expTypes.OPERATOR,
      values: left !== undefined ? [operator, left, right] : [operator, right],
    }
  },
)

astParsers.A_Indirection = checkProps(['arg', 'indirection'], (exp, context) => {
  const { arg, indirection } = exp
  return {
    type: expTypes.OPERATOR,
    values: ['[]', parseASTNode(arg, context), ...indirection.map(e => parseASTNode(e, context))],
  }
})

astParsers.BoolExpr = checkProps(['boolop', 'args'], ({ boolop, args }, context) => ({
  type: expTypes.OPERATOR,
  values: [boolop.slice(0, -5).toLowerCase(), ...args.map(e => parseASTNode(e, context))],
}))

astParsers.NullTest = checkProps(['arg', 'nulltesttype'], ({ arg, nulltesttype }, context) => {
  const operator = nulltesttype === 'IS_NOT_NULL' ? 'IS NOT' : 'IS'
  return { type: expTypes.OPERATOR, values: [operator, parseASTNode(arg, context), null] }
})

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

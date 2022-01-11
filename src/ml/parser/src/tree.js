/* eslint-disable no-use-before-define */
const {
  isNull,
  isNonNull,
  isString,
  isArray,
  isInt,
  isObjectExpression,
  sanitizeString,
  sanitizeAlias,
  sanitizeCast,
  escapeLiteral,
  escapeIdentifier,
  trimSQL,
  parserError,
  expressionTypes: expTypes,
  isNonArrayObject,
} = require('./utils')
const { isShortExpression, parseShortExpression } = require('./short')
const functions = require('./functions')
const operators = require('./operators')


const columnRefRE = /^\w+.\w+$/

// used for 'for' and 'joins'
// has side effect -> adds identifier to context
const parseViewExpression = (exp, context) => {
  if (isString(exp)) {
    if (isShortExpression(exp, 'view')) {
      return parseShortExpression(exp)
    }
    return parseExpression({ type: expTypes.VIEW, view: exp }, context)
  }
  if (isObjectExpression(exp, expTypes.SELECT)) {
    return parseExpression({ ...exp, type: expTypes.SELECT_RANGE }, context)
  }
  if (isObjectExpression(exp, expTypes.VIEW) || isObjectExpression(exp, expTypes.SELECT_RANGE)) {
    return parseExpression(exp, context)
  }
  throw parserError(`Invalid view identifier/subquery syntax: ${exp}`)
}

// used for 'with'
// has side effect -> adds identifier to context
const parseCTEExpression = (exp, context) => {
  if (isObjectExpression(exp, expTypes.SELECT_CTE)) {
    return parseExpression(exp, context)
  }
  if (isObjectExpression(exp, expTypes.SELECT)) {
    return parseExpression({ ...exp, type: expTypes.SELECT_CTE }, context)
  }
  throw parserError(`Invalid with syntax: ${exp}`)
}

const parseJoinExpression = (exp, context) => {
  if (isObjectExpression(exp, expTypes.JOIN)) {
    return parseExpression(exp, context)
  }
  if (isNonArrayObject(exp)) {
    return parseExpression({ ...exp, type: expTypes.JOIN }, context)
  }
  throw parserError(`Invalid join syntax: ${exp}`)
}

const applyExpressions = (conditions, context, cb) => {
  try {
    // try to parse as expression
    return cb(parseExpression(conditions, context))
  } catch (err) {
    if (!isArray(conditions)) {
      throw err
    }
    // try to parse each array element individually
    return conditions.forEach(condition => cb(parseExpression(condition, context)))
  }
}

class Node {
  constructor(exp, context) {
    if (!isObjectExpression(exp)) {
      throw parserError(`Expression must be an object: ${exp}`)
    }
    this._context = context || { views: {}, ctes: {}, refs: {}, params: {} }
    // as property
    if (this.constructor.aliasable) {
      this.as = sanitizeAlias(exp.as) // basic validation
    }
    // cast property
    if (this.constructor.castable) {
      this.cast = sanitizeCast(exp.cast) // basic validation
    }
  }

  _applyAlias(sql) {
    return this.as ? `${sql} AS ${escapeIdentifier(this.as)}` : sql
  }

  _applyCast(sql) {
    return this.cast ? `CAST(${sql} AS ${this.cast})` : sql
  }

  // to be implemented by child class
  _toSQL() {
    throw parserError(`_toSQL() must be implemented in element ${this.constructor.name}`)
  }

  toSQL() {
    let sql = this._toSQL()
    if (this.constructor.castable) {
      sql = this._applyCast(sql)
    }
    if (this.constructor.aliasable) {
      sql = this._applyAlias(sql)
    }
    return trimSQL(sql)
  }

  // to be implemented by child class
  _toQL() {
    throw parserError(`_toQL() must be implemented in element ${this.constructor.name}`)
  }

  toQL() {
    const ql = this._toQL()
    if (this.constructor.castable && this.cast) {
      ql.cast = this.cast
    }
    if (this.constructor.aliasable && this.as) {
      ql.as = this.as
    }
    return ql
  }

  to(name, ...args) {
    // eslint-disable-next-line no-prototype-builtins
    if (!Object.hasOwnProperty('_parsers') || !(name in this.constructor._parsers)) {
      throw parserError(`Parser "${name}" not registered on node type: ${this.constructor.name}`)
    }
    return this.constructor._parsers(this, ...args)
  }

  static registerParser(name, parser) {
    // eslint-disable-next-line no-prototype-builtins
    if (!this.hasOwnProperty('_parsers')) {
      // _parsers cannot be inherited
      this._parsers = {}
    }
    this._parsers[name] = parser
  }
}
Node.aliasable = true
Node.castable = true

class SelectNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const {
      with: ctes,
      from,
      joins,
      distinct,
      columns,
      where,
      having,
      groupBy,
      orderBy,
      limit,
      offset,
      as,
    } = exp

    const parentContext = this._context
    // local context
    this._context = {
      ...parentContext,
      ctes: { ...parentContext.ctes }, // shallow copy to prevent from adding to parent
      refs: {}, // views/subs/cte's in use in scope
    }

    // WITH
    this.with = []
    if (isNonNull(ctes)) {
      if (!isArray(ctes, { minLength: 1 })) {
        throw parserError(`Invalid with syntax: ${ctes}`)
      }
      this.with = ctes.map(e => parseCTEExpression(e, this._context))
    }

    // FROM
    // string, select object or undefined/null (e.g. select true)
    this.from = undefined
    if (isNonNull(from)) {
      this.from = parseViewExpression(from, this._context)
    }

    // JOINS
    this.joins = []
    if (isNonNull(joins)) {
      if (!isArray(joins, { minLength: 1 })) {
        throw parserError(`Invalid join syntax: ${joins}`)
      }
      this.joins = joins.map(e => parseJoinExpression(e, this._context))
    }

    // DISTINCT
    this.distinct = false
    if (isNonNull(distinct)) {
      if (typeof distinct !== 'boolean') {
        throw parserError(`Invalid distinct syntax: ${distinct}`)
      }
      this.distinct = distinct
    }

    // COLUMNS
    this.columns = []
    if (isNull(columns)) {
      throw parserError('Missing columns object in select expression')
    }
    applyExpressions(columns, this._context, e => this.columns.push(e))

    // WHERE
    this.where = []
    if (isNonNull(where)) {
      applyExpressions(where, this._context, e => this.where.push(e))
    }

    // HAVING
    this.having = []
    if (isNonNull(having)) {
      applyExpressions(having, this._context, e => this.having.push(e))
    }

    // GROUP BY
    this.groupBy = []
    if (isNonNull(groupBy)) {
      applyExpressions(groupBy, this._context, e => this.groupBy.push(e))
    }

    // ORDER BY
    this.orderBy = []
    if (isNonNull(orderBy)) {
      applyExpressions(orderBy, this._context, e => this.orderBy.push(e))
    }

    // LIMIT
    this.limit = undefined
    if (isNonNull(limit) && sanitizeString(limit) !== 'all') {
      if (!isInt(limit, 0)) {
        throw parserError(`Invalid limit: ${limit}`)
      }
      this.limit = limit
    }

    // OFFSET
    this.offset = undefined
    if (isNonNull(offset)) {
      if (!isInt(offset, 0)) {
        throw parserError(`Invalid offset: ${offset}`)
      }
      this.offset = offset
    }

    // ALIAS - no collision with cte/subquery in scope allowed (even if inherited from parent)
    if (isNonNull(as) && (as in parentContext.refs || as in parentContext.ctes)) {
      throw parserError(`Alias already in use: ${as}`)
    }
  }

  _toSQL() {
    const ctes = this.with.length
      ? `WITH ${this.with.map(e => e.toSQL()).join(', ')}`
      : ''

    const distinct = this.distinct ? 'DISTINCT' : ''
    const columns = this.columns.map(e => e.toSQL()).join(', ')

    const from = this.from ? `FROM ${this.from.toSQL()}` : ''

    const joins = this.joins.length
      ? this.joins.map(e => e.toSQL()).join(', ')
      : ''

    const where = this.where.length ?
      `WHERE ${this.where.map(e => e.toSQL()).join(' AND ')}`
      : ''

    const having = this.having.length
      ? `HAVING ${this.having.map(e => e.toSQL()).join(' AND ')}`
      : ''

    const groupBy = this.groupBy.length
      ? `GROUP BY ${this.groupBy.map(e => e.toSQL()).join(', ')}`
      : ''

    const orderBy = this.orderBy.length
      ? `ORDER BY ${this.orderBy.map(e => e.toSQL()).join(', ')}`
      : ''

    const limit = this.limit !== undefined ? `LIMIT ${this.limit}` : ''
    const offset = this.offset !== undefined ? `OFFSET ${this.offset}` : ''

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

  _toQL() {
    return {
      type: expTypes.SELECT,
      with: this.with.length ? this.with.map(e => e.toQL()) : undefined,
      from: this.from ? this.from.toQL() : undefined,
      joins: this.joins.length ? this.joins.map(e => e.toQL()) : undefined,
      distinct: this.distinct || undefined,
      columns: this.columns.map(e => e.toQL()),
      where: this.where.length ? this.where.map(e => e.toQL()) : undefined,
      having: this.having.length ? this.having.map(e => e.toQL()) : undefined,
      groupBy: this.groupBy.length ? this.groupBy.map(e => e.toQL()) : undefined,
      orderBy: this.orderBy.length ? this.orderBy.map(e => e.toQL()) : undefined,
      limit: this.limit,
      offset: this.offset,
    }
  }

  // eslint-disable-next-line class-methods-use-this
  _applyAlias(sql) {
    // parent/child class is responsible for implementing the alias syntax
    return sql
  }
}

class CTESelectNode extends SelectNode {
  constructor(exp, context) {
    const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing with alias: ${as}`)
    }
    // register cte against parent context
    parentContext.ctes[as] = true
  }

  _applyAlias(sql) {
    return `${escapeIdentifier(this.as)} AS (${sql})`
  }
}
CTESelectNode.castable = false

class ViewSelectNode extends SelectNode {
  constructor(exp, context) {
    const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing subquery alias: ${as}`)
    }
    // register identifier against parent context
    parentContext.refs[as] = true
  }

  _applyAlias(sql) {
    return `(${sql}) AS ${escapeIdentifier(this.as)}`
  }
}
ViewSelectNode.castable = false

class JoinNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { joinType, view, on } = exp
    if (!['inner', 'left', 'right'].includes(sanitizeString(joinType))) {
      throw parserError(`Invalid join type: ${joinType}`)
    }
    this.joinType = joinType
    this.view = parseViewExpression(view, this._context)
    this.on = parseExpression(on, this._context)
  }

  _toSQL() {
    return `${this.joinType} JOIN ${this.view.toSQL()} ON ${this.on.toSQL()}`
  }

  _toQL() {
    return {
      // type: expressionTypes.JOIN,
      joinType: this.joinType,
      view: this.view.toQL(),
      on: this.on.toQL(),
    }
  }
}
JoinNode.aliasable = false
JoinNode.castable = false

class ViewReferenceNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { view, as } = exp
    // view must be accessible from local scope
    if (!isString(view, true)) {
      throw parserError(`Invalid view reference expression: ${view}`)
    }
    const ref = as || view
    // check that identifier not already in use by any subquery/cte in scope
    if (ref in this._context.refs || ref in this._context.ctes) {
      throw parserError(`View identifier already in use: ${view}`)
    }
    this.view = view
    // register identifier against local context
    this._context.refs[ref] = view in this._context.ctes || view
  }

  _toSQL() {
    return escapeIdentifier(this.view)
  }

  _toQL() {
    return {
      type: expTypes.VIEW,
      view: this.view,
    }
  }
}
ViewReferenceNode.castable = false

class ColumnReferenceNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { column, view, as } = exp
    if (
      !isString(column, true)
      || (
        !(isString(view, true) && view in this._context.refs)
        && !(isNull(view) && Object.keys(this._context.refs).length === 1)
      )
    ) {
      throw parserError(`Invalid column expression: ${exp}`)
    }
    if (isNonNull(as) && column === '*') {
      throw parserError(`Invalid column alias: ${as}`)
    }
    if (as === column) {
      this.as = undefined
    }
    this.column = column
    this.view = isNull(view) ? Object.keys(this._context.refs)[0] : view
    // register view + column in global context
    const viewRef = this._context.refs[this.view]
    if (viewRef !== true) {
      this._context.views[viewRef] = this._context.views[viewRef] || {}
      if (!('*' in this._context.views[viewRef])) {
        if (column === '*') {
          // clear other columns when *
          this._context.views[viewRef] = {}
        }
        this._context.views[viewRef][column] = true
      }
    }
  }

  _toSQL() {
    const column = this.column === '*' ? '*' : escapeIdentifier(this.column)
    return `${escapeIdentifier(this.view)}.${column}`
  }

  _toQL() {
    return { type: expTypes.COLUMN, column: this.column, view: this.view }
  }
}

class ParameterReferenceNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true) || value.startsWith('__')) {
      throw parserError(`Invalid parameter: ${value}`)
    }
    this.name = value.toLowerCase()
    this._context.params[this.name] = true
  }

  _toSQL() {
    return `@param('${this.name}')`
  }

  _toQL() {
    return { type: expTypes.PARAMETER, value: this.name }
  }
}

class ArrayNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid array syntax: ${exp}`)
    }
    this.values = exp.values.map(e => parseExpression(e, this._context))
  }

  _toSQL() {
    return `ARRAY[${this.values.map(e => e.toSQL()).join(', ')}]`
  }

  _toQL() {
    return {
      type: expTypes.ARRAY,
      values: this.values.map(e => e.toQL()),
    }
  }
}

class ListNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid list syntax: ${exp}`)
    }
    this.values = exp.values.map(e => parseExpression(e, this._context))
  }

  _toSQL() {
    return `(${this.values.map(e => e.toSQL()).join(', ')})`
  }

  _toQL() {
    return {
      type: expTypes.LIST,
      values: this.values.map(e => e.toQL()),
    }
  }
}

class FunctionNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid function syntax: ${exp}`)
    }
    const [name, ...args] = exp.values
    this.name = sanitizeString(name)
    const fn = functions[this.name]
    if (!fn) {
      throw parserError(`Invalid function: ${name}`)
    }
    const { argsLength, minArgsLength, maxArgsLength, defaultCast } = fn
    this.args = (args || []).map(e => parseExpression(e, this._context))
    if (
      argsLength !== undefined
        ? this.args.length !== argsLength
        : (
          (minArgsLength && this.args.length < minArgsLength)
          || (maxArgsLength !== undefined && this.args.length > maxArgsLength)
        )
    ) {
      throw parserError(`Too few or too many arguments in function: ${this.name}`)
    }
    this.defaultCast = defaultCast
  }

  _toSQL() {
    return `${this.name}(${this.args.map(e => e.toSQL()).join(', ')})`
  }

  _applyCast(sql) {
    const cast = this.cast || this.defaultCast
    return cast ? `CAST(${sql} AS ${cast})` : sql
  }

  _toQL() {
    return {
      type: expTypes.FUNCTION,
      values: [this.name, ...this.args.map(e => e.toQL())],
    }
  }
}

class OperatorNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    // binary and left-unary operators
    if (!isArray(exp.values, { minLength: 2 })) {
      throw parserError(`Invalid operator syntax: ${exp}`)
    }
    const [name, ...operands] = exp.values
    this.name = sanitizeString(name)
    const op = operators[this.name]
    if (!op) {
      throw parserError(`Invalid operator: ${name}`)
    }
    if ((this.name === 'between' || this.name === 'not between') && operands.length !== 3) {
      throw parserError(`Invalid number of operands for operator: ${this.name}`)
    }
    this.operands = operands.map(e => parseExpression(e, this._context))
  }

  _toSQL() {
    if (this.name === 'between' || this.name === 'not between') {
      const [oA, oB, oC] = this.operands
      return `(${oA.toSQL()} ${this.name} ${oB.toSQL()} AND ${oC.toSQL()})`
    }
    return `(${this.operands.map((o, i, all) => {
      const op = i > 0 || all.length === 1 ? `${this.name} ` : ''
      return op + o.toSQL()
    }).join(' ')})`
  }

  _toQL() {
    return {
      type: expTypes.OPERATOR,
      values: [this.name, ...this.operands.map(e => e.toQL())],
    }
  }
}

class CastNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (isNull(this.cast) || value === undefined) {
      throw parserError(`Invalid casting syntax: ${exp}`)
    }
    this.value = parseExpression(value, this._context)
    if (!this.value.constructor.castable) {
      throw parserError(`Illegal casting: ${this.cast}`)
    }
    // collapse if possible
    if (!this.value.cast && (!this.as || this.value.constructor.aliasable)) {
      if (this.as) {
        this.value.as = this.as
      }
      this.value.cast = this.cast
      return this.value
    }
  }

  _toSQL() {
    return this.value.toSQL()
  }

  _toQL() {
    return {
      type: expTypes.CAST,
      value: this.value.toQL(),
    }
  }
}

class SQLNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true)) {
      throw parserError(`Invalid sql syntax: ${exp}`)
    }
    // parse from sql first
    // INSERT SQL TO QL HERE
    this.value = parseExpression(value, this._context)
    if (this.cast && !this.value.constructor.castable) {
      throw parserError(`Illegal casting: ${this.cast}`)
    }
    if (this.as && !this.value.constructor.aliasable) {
      throw parserError(`Illegal aliasing: ${this.as}`)
    }

    this.value.as = this.as || this.value.as
    // collapse if possible
    if (!this.cast || !this.value.cast) {
      if (this.cast) {
        this.value.cast = this.cast
      }
      if (this.as) {
        this.as.value = this.as
      }
      return this.value
    }
  }

  _toSQL() {
    return this.value.toSQL()
  }

  _toQL() {
    return {
      type: expTypes.CAST,
      value: this.value.toQL(),
    }
  }
}

class PrimitiveNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!['string', 'boolean', 'number'].includes(typeof value) && value !== null) {
      throw parserError(`Invalid primitive: ${value}`)
    }
    this.value = value
  }

  _toSQL() {
    return typeof this.value === 'string' ? escapeLiteral(this.value) : String(this.value)
  }

  _toQL() {
    // return { type: expTypes.PRIMITIVE, value: this.value }
    if (this.as || this.cast) {
      return { type: expTypes.PRIMITIVE, value: this.value }
    }
    return this.value
  }
}

class CaseNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid case syntax: ${exp}`)
    }
    const cases = [...exp.values]
    // first item is either the default result or a cond/res pair
    this.defaultRes = !isArray(cases[0])
      ? parseExpression(cases.shift(), this._context)
      : undefined
    this.cases = cases.map(([cond, res]) => {
      if (isNull(cond) || res === undefined) {
        throw parserError(`Invalid case syntax: ${exp}`)
      }
      return [cond, res].map(e => parseExpression(e, this._context))
    })
  }

  _toSQL() {
    return `
      CASE
        ${this.cases.map(([cond, res]) => `WHEN ${cond.toSQL()} THEN ${res.toSQL()}`).join('\n')}
        ${this.defaultRes ? `ELSE ${this.defaultRes.toSQL()}` : ''}
      END
    `
  }

  _toQL() {
    const cases = this.cases.map(c => c.map(e => e.toQL()))
    return {
      type: expTypes.CASE,
      values: this.defaultRes ? [this.defaultRes.toQL(), ...cases] : cases,
    }
  }
}

class SortNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value, direction, nulls } = exp
    if (!value || value === true) {
      throw parserError(`Invalid sorting syntax: ${exp}`)
    }
    this.direction = undefined
    if (isNonNull(direction)) {
      const safeDirection = sanitizeString(direction)
      if (!['asc', 'desc'].includes(safeDirection)) {
        throw parserError(`Invalid sorting direction syntax: ${exp}`)
      }
      this.direction = safeDirection
    }
    this.nulls = undefined
    if (isNonNull(nulls)) {
      const safeNulls = sanitizeString(nulls)
      if (!['first', 'last'].includes(safeNulls)) {
        throw parserError(`Invalid sorting nulls syntax: ${exp}`)
      }
      this.nulls = safeNulls
    }
    this.value = parseExpression(value, this._context)
  }

  _toSQL() {
    const direction = this.direction ? ` ${this.direction}` : ''
    const nulls = this.nulls ? ` NULLS ${this.nulls}` : ''
    return this.value.toSQL() + direction + nulls
  }

  _toQL() {
    return {
      type: expTypes.SORT,
      value: this.value.toQL(),
      direction: this.direction,
      nulls: this.nulls,
    }
  }
}
SortNode.aliasable = false
SortNode.castable = false

const nodes = {
  SelectNode,
  ViewSelectNode,
  CTESelectNode,
  JoinNode,
  ViewReferenceNode,
  ColumnReferenceNode,
  ParameterReferenceNode,
  CastNode,
  PrimitiveNode,
  CaseNode,
  ArrayNode,
  ListNode,
  FunctionNode,
  SortNode,
  OperatorNode,
}

/** @type {Object.<string, (exp, context: { views, ctes, refs, params }) => Node} */
const objectParsers = {}
// has side effect
objectParsers[expTypes.SELECT] = (exp, context) => new SelectNode(exp, context)
objectParsers[expTypes.SELECT_RANGE] = (exp, context) => new ViewSelectNode(exp, context)
objectParsers[expTypes.SELECT_CTE] = (exp, context) => new CTESelectNode(exp, context)
objectParsers[expTypes.JOIN] = (exp, context) => new JoinNode(exp, context)
objectParsers[expTypes.VIEW] = (exp, context) => new ViewReferenceNode(exp, context)
objectParsers[expTypes.COLUMN] = (exp, context) => new ColumnReferenceNode(exp, context)
objectParsers[expTypes.PARAMETER] = (exp, context) => new ParameterReferenceNode(exp, context)
// objectParsers[expTypes.SQL] = (exp, context) => new SQLNode(exp, context)
objectParsers[expTypes.CAST] = (exp, context) => new CastNode(exp, context)
objectParsers[expTypes.PRIMITIVE] = (exp, context) => new PrimitiveNode(exp, context)
objectParsers[expTypes.CASE] = (exp, context) => new CaseNode(exp, context)
objectParsers[expTypes.ARRAY] = (exp, context) => new ArrayNode(exp, context)
objectParsers[expTypes.LIST] = (exp, context) => new ListNode(exp, context)
objectParsers[expTypes.FUNCTION] = (exp, context) => new FunctionNode(exp, context)
objectParsers[expTypes.SORT] = (exp, context) => new SortNode(exp, context)
objectParsers[expTypes.OPERATOR] = (exp, context) => new OperatorNode(exp, context)
objectParsers[expTypes.AND] = (exp, context) => {
  const { values, as, cast } = exp
  if (!isArray(values)) {
    throw parserError(`Invalid and syntax: ${exp}`)
  }
  return new OperatorNode({ values: ['and', ...values], as, cast }, context)
}
objectParsers[expTypes.OR] = (exp, context) => {
  const { values, as, cast } = exp
  if (!isArray(values)) {
    throw parserError(`Invalid or syntax: ${exp}`)
  }
  return new OperatorNode({ values: ['or', ...values], as, cast }, context)
}

const parseObjectExpression = (exp, context) => {
  const type = sanitizeString(exp.type, true)
  const parser = objectParsers[type]
  if (!parser) {
    throw parserError(`Invalid object expression type: ${type}`)
  }
  return parser(exp, context)
}

const parseArrayExpression = (exp, context) => {
  // single expression: [expression]
  if (exp.length === 1) {
    return parseExpression(exp[0], context)
  }

  if (exp.length === 2) {
    const [column, view] = exp
    return parseExpression({ type: expTypes.COLUMN, column, view }, context)
  }

  if (exp.length === 3) {
    try {
      // column
      const [column, view, as] = exp
      return parseExpression({ type: expTypes.COLUMN, column, view, as }, context)
    } catch (_) {
      // condition/operator
      const [oA, operator, oB] = exp
      return parseExpression({ type: expTypes.OPERATOR, values: [operator, oA, oB] }, context)
    }
  }

  if (exp.length === 4) {
    // condition/operator
    const [argA, operator, argB, argC] = exp
    return parseExpression(
      { type: expTypes.OPERATOR, values: [operator, argA, argB, argC] },
      context,
    )
  }
  throw parserError('Invalid array expression')
}

const parseExpression = (exp, context) => {
  switch (typeof exp) {
    case 'string':
      try {
        if (exp.toLowerCase() === 'null') {
          return parseExpression(null)
        }
        if (columnRefRE.test(exp)) {
          const [column, view] = exp.split('.')
          return parseExpression({ type: expTypes.COLUMN, column, view }, context)
        }
        if (isShortExpression(exp)) {
          const objExp = parseShortExpression(exp)
          return parseExpression(objExp, context)
        }
        return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
      } catch (_) {
        return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
      }

    case 'boolean':
    case 'number':
      return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)

    case 'object':
      // NULL value
      if (exp === null) {
        return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
      }
      // array expression
      if (isArray(exp)) {
        return parseArrayExpression(exp, context)
      }
      // object expression
      return parseObjectExpression(exp, context)

    default:
      throw parserError(`Invalid expression: ${exp}`)
  }
}

module.exports = {
  nodes,
  parseExpression,
}

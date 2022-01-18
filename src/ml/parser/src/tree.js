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
const { isShortExpression, parseShortExpression, sanitizeShortExpression } = require('./short')
const { parseSQLExpression } = require('./sql')
const functions = require('./functions')
const operators = require('./operators')


const columnRefRE = /^\w+.\w+$/

const addToViewColumns = (viewColumns = {}, columnOrColumns) => {
  const columns = isString(columnOrColumns) ? { [columnOrColumns]: true } : columnOrColumns
  if (!('*' in viewColumns || '*' in columns)) {
    return Object.assign(viewColumns, columns)
  }
  return { '*': true }
}

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
  throw parserError(`Invalid view identifier/subquery syntax: ${JSON.stringify(exp)}`)
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
  throw parserError(`Invalid with syntax: ${JSON.stringify(exp)}`)
}

const parseJoinExpression = (exp, context) => {
  if (isObjectExpression(exp, expTypes.JOIN)) {
    return parseExpression(exp, context)
  }
  if (isNonArrayObject(exp)) {
    return parseExpression({ ...exp, type: expTypes.JOIN }, context)
  }
  throw parserError(`Invalid join syntax: ${JSON.stringify(exp)}`)
}

/* context
 - refs: scoped to nearest parent SELECT node (or current node is SELECT)
 - ctes: scoped to nearest parent SELECT (inherits from higher scopes)
 - views: scoped to current node (updates must be propagated to parents)
 - params: scoped to current node (updates must be propagated to parents)
 - options: global
*/

const nodeOptions = {
  parameters: undefined,
  _safe: true,
}

const parserOptions = {
  keepShorts: true,
  keepParamRefs: false,
  parameters: undefined,
  _safe: true,
}

class Node {
  constructor(exp, context) {
    if (!isObjectExpression(exp)) {
      throw parserError(`Expression must be an object: ${JSON.stringify(exp)}`)
    }
    // refs and ctes can be booked on parent or node (shared with siblings)
    // params and views are booked on node and must be propagated to parent

    this._parentContext = context || {}
    if (!context || !context.options || !context.options._safe) {
      this._parentContext.options = { ...nodeOptions, ...((context && context.options) || {}) }
    }
    this._context = {
      ...this._parentContext,
      views: {},
      params: {},
      _parentContext: this._parentContext,
    }
    this._memo = {}
    // as property
    if (this.constructor.aliasable) {
      this.as = sanitizeAlias(exp.as) // basic validation
      this._aliasIsUpdatable = true
    } else if (exp.as !== undefined) {
      throw parserError(`Illegal aliasing: ${exp.as}`)
    }
    // cast property
    if (this.constructor.castable) {
      this.cast = sanitizeCast(exp.cast) // basic validation
      this._castIsUpdatable = true
    } else if (exp.cast !== undefined) {
      throw parserError(`Illegal casting: ${exp.cast}`)
    }
  }

  get viewColumns() {
    return Object.entries(this._context.views).reduce((acc, [view, columns]) => {
      acc[view] = new Set(Object.keys(columns))
      return acc
    }, {})
  }

  get parameters() {
    return new Set(Object.keys(this._context.params))
  }

  hasParameterValues() {
    return this._context.options.parameters && this.parameters.size > 0
  }

  isRoot() {
    return Object.keys(this._parentContext).every(k => k !== 'options')
  }

  hasSelectAncestor() {
    return 'refs' in this._parentContext && 'ctes' in this._parentContext
  }

  get aliasIsUpdatable() {
    return this.constructor.aliasable && this._aliasIsUpdatable
  }

  get castIsUpdatable() {
    return this.constructor.castable && this._castIsUpdatable
  }

  _validateCastAndAliasLayer(cast, as) {
    if (!cast && !as) {
      return
    }
    if (this.as || this._as) {
      throw parserError(`Invalid alias: ${this.as || this._as}`)
    }
    if (cast) {
      if (!this.castIsUpdatable) {
        throw parserError(`Illegal casting: ${cast}`)
      }
      return
    }
    if (!this.aliasIsUpdatable) {
      throw parserError(`Illegal aliasing: ${as}`)
    }
  }

  _populateCastAndAliasProxies(value) {
    if (this.cast) {
      return
    }
    if (!this.as) {
      this._as = value.as || value._as
      this._aliasIsUpdatable = value.aliasIsUpdatable
    }
    this._cast = value.cast || value._cast
    this._castIsUpdatable = value.castIsUpdatable
  }

  _applyCastAndAliasLayer(cast, as) {
    this._validateCastAndAliasLayer(cast, as)
    if (this.cast && cast) {
      return false
    }
    this.cast = cast || this.cast
    this.as = cast ? as : as || this.as
    return true
  }

  // writes to parent context
  _propagateContext(key, depth = 1) {
    let context = this._context
    for (let i = 0; depth === -1 || i < depth; i++) {
      const parentContext = context._parentContext
      if (!(key in parentContext && key in context)) {
        return
      }
      if (key === 'views') {
        Object.entries(context.views).forEach(([view, cols]) => {
          parentContext.views[view] = addToViewColumns(parentContext.views[view], cols)
        })
      } else {
        // extends parent context with child values, overrides parent when conflicting keys
        Object.assign(parentContext[key], context[key])
      }
      context = parentContext
    }
  }

  _registerRef(name, view) {
    if (!this.hasSelectAncestor()) {
      // expression evaluated in isolation
      return
    }
    if (name in this._parentContext.refs || name in this._parentContext.ctes) {
      throw parserError(`Identifier already in use: ${name}`)
    }
    // register identifier against parent context
    this._parentContext.refs[name] = view in this._parentContext.ctes || view || true
  }

  _registerCTE(name) {
    if (!this.hasSelectAncestor()) {
      // expression evaluated in isolation
      return
    }
    if (name in this._parentContext.refs || name in this._parentContext.ctes) {
      throw parserError(`Identifier already in use: ${name}`)
    }
    // allowed to override inherited cte
    if (
      name in this._parentContext.refs
      || (
        name in this._parentContext.ctes
        && (
          !(name in this._parentContext._parentContext.ctes)
          || this._parentContext._parentContext.ctes[name] !== this._parentContext.ctes[name]
        )
      )
    ) {
      throw parserError(`Identifier already in use: ${name}`)
    }
    // register cte against parent context
    this._parentContext.ctes[name] = {}
  }

  _registerParam(name) {
    this._context.params[name] = true
    this._propagateContext('params', -1)
  }

  _registerViewColumn(view, column) {
    if (!this.hasSelectAncestor()) {
      // expression evaluated in isolation
      this._context.views[view] = addToViewColumns(this._context.views[view], column)
    } else {
      const viewRef = this._context.refs[view]
      if (viewRef !== true) {
        this._context.views[viewRef] = addToViewColumns(this._context.views[viewRef], column)
      }
    }
    this._propagateContext('views', -1)
  }

  _applyAliasToSQL(sql) {
    return `${sql} AS ${escapeIdentifier(this.as)}`
  }

  _applyCastToSQL(sql) {
    return `CAST(${sql} AS ${this.cast})`
  }

  // to be implemented by child class
  _toSQL() {
    throw parserError(`_toSQL() must be implemented in node ${this.constructor.name}`)
  }

  toSQL(options) {
    // if ('sql' in this._memo) {
    //   return this._memo.sql
    // }
    const safeOptions = !options || !options._safe
      ? { ...parserOptions, ...(options || {}) }
      : options
    let sql = this._toSQL(safeOptions)
    if (this.constructor.castable && this.cast) {
      sql = this._applyCastToSQL(sql)
    }
    if (this.constructor.aliasable && this.as) {
      sql = this._applyAliasToSQL(sql)
    }
    // this._memo.sql = trimSQL(sql)
    // return this._memo.sql
    return trimSQL(sql)
  }

  // to be implemented by child class
  _toQL() {
    throw parserError(`_toQL() must be implemented in node ${this.constructor.name}`)
  }

  // return value is immutable
  toQL(options) {
    // if ('ql' in this._memo) {
    //   return this._memo.ql
    // }
    const safeOptions = !options || !options._safe
      ? { ...parserOptions, ...(options || {}) }
      : options
    const ql = this._toQL(safeOptions)
    if (this.constructor.castable && this.cast) {
      ql.cast = this.cast
    }
    if (this.constructor.aliasable && this.as) {
      ql.as = this.as
    }
    // this._memo.ql = Object.freeze(ql)
    return ql
  }

  // to be implemented by child class
  _toShort() {
    throw parserError(`Node ${this.constructor.name} is not parsable into a short expression`)
  }

  toShort(options) {
    const safeOptions = !options || !options._safe
      ? { ...parserOptions, ...(options || {}) }
      : options
    const short = this._toShort(safeOptions)
    if (isString(short)) {
      return short
    }
    const { name, args } = short
    const namedArgs = Object.entries(args).reduce((acc, [k, v]) => {
      if (v !== undefined) {
        // json stringify any value other than short
        acc.push(`${k}=${isString(v) && isShortExpression(v) ? v : JSON.stringify(v)}`)
      }
      return acc
    }, [])
    return `@${name}(${namedArgs.join(',')})`
  }

  // return value is immutable
  to(name, ...args) {
    // eslint-disable-next-line no-prototype-builtins
    if (!Object.hasOwnProperty('_parsers') || !(name in this.constructor._parsers)) {
      throw parserError(`Parser "${name}" not registered on node type: ${this.constructor.name}`)
    }
    const memo = `parser:${name}`
    if (memo in this._memo) {
      return this._memo[memo]
    }
    this._memo[memo] = this.constructor._parsers(this, ...args)
    if (typeof this._memo[memo] === 'object') {
      this._memo[memo] = Object.freeze(this._memo[memo])
    }
    return this._memo[memo]
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
    } = exp

    this._context = {
      ...this._context,
      ctes: { ...(this._context.ctes || {}) }, // shallow copy to prevent from adding to parent
      refs: {}, // views/subs/cte's in use in scope
    }

    // WITH
    this.with = []
    if (isNonNull(ctes)) {
      if (!isArray(ctes)) {
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
      if (!isArray(joins)) {
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
    if (!isArray(columns, { minLength: 1 })) {
      throw parserError('Missing columns in select expression')
    }
    this.columns = columns.map(e => parseExpression(e, this._context))

    // WHERE
    this.where = []
    if (isNonNull(where)) {
      if (!isArray(where)) {
        console.log('where', where)
        throw parserError(`Invalid where syntax: ${where}`)
      }
      this.where = where.map(e => parseExpression(e, this._context))
    }

    // HAVING
    this.having = []
    if (isNonNull(having)) {
      if (!isArray(having)) {
        throw parserError(`Invalid having syntax: ${having}`)
      }
      this.having = having.map(e => parseExpression(e, this._context))
    }

    // GROUP BY
    this.groupBy = []
    if (isNonNull(groupBy)) {
      if (!isArray(groupBy)) {
        throw parserError(`Invalid groupBy syntax: ${groupBy}`)
      }
      this.groupBy = groupBy.map(e => parseExpression(e, this._context))
    }

    // ORDER BY
    this.orderBy = []
    if (isNonNull(orderBy)) {
      if (!isArray(orderBy)) {
        throw parserError(`Invalid orderBy syntax: ${orderBy}`)
      }
      this.orderBy = orderBy.map(e => parseExpression(e, this._context))
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

    // no casting/aliasing for top-level select
    if (!this.hasSelectAncestor()) {
      if (this.cast) {
        throw parserError(`Illegal casting: ${this.cast}`)
      }
      if (this.as) {
        throw parserError(`Illegal aliasing: ${this.cast}`)
      }
      this._castIsUpdatable = false
      this._aliasIsUpdatable = false
    }
  }

  _toSQL(options) {
    const ctes = this.with.length
      ? `WITH ${this.with.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const distinct = this.distinct ? 'DISTINCT' : ''
    const columns = this.columns.map(e => e.toSQL(options)).join(', ')

    const from = this.from ? `FROM ${this.from.toSQL(options)}` : ''

    const joins = this.joins.length
      ? this.joins.map(e => e.toSQL(options)).join(', ')
      : ''

    const where = this.where.length ?
      `WHERE ${this.where.map(e => e.toSQL(options)).join(' AND ')}`
      : ''

    const having = this.having.length
      ? `HAVING ${this.having.map(e => e.toSQL(options)).join(' AND ')}`
      : ''

    const groupBy = this.groupBy.length
      ? `GROUP BY ${this.groupBy.map(e => e.toSQL(options)).join(', ')}`
      : ''

    const orderBy = this.orderBy.length
      ? `ORDER BY ${this.orderBy.map(e => e.toSQL(options)).join(', ')}`
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

  _toQL(options) {
    return {
      type: expTypes.SELECT,
      with: this.with.length ? this.with.map(e => e.toQL(options)) : undefined,
      from: this.from ? this.from.toQL(options) : undefined,
      joins: this.joins.length ? this.joins.map(e => e.toQL(options)) : undefined,
      distinct: this.distinct || undefined,
      columns: this.columns.map(e => e.toQL(options)),
      where: this.where.length ? this.where.map(e => e.toQL(options)) : undefined,
      having: this.having.length ? this.having.map(e => e.toQL(options)) : undefined,
      groupBy: this.groupBy.length ? this.groupBy.map(e => e.toQL(options)) : undefined,
      orderBy: this.orderBy.length ? this.orderBy.map(e => e.toQL(options)) : undefined,
      limit: this.limit,
      offset: this.offset,
    }
  }

  // eslint-disable-next-line class-methods-use-this
  _applyAliasToSQL(sql) {
    // parent/child class is responsible for implementing the alias syntax
    return sql
  }
}

class CTESelectNode extends SelectNode {
  constructor(exp, context) {
    // const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing with alias: ${as}`)
    }
    // register cte against parent context
    this._registerCTE(as)
  }

  _applyAliasToSQL(sql) {
    return `${escapeIdentifier(this.as)} AS (${sql})`
  }
}
CTESelectNode.castable = false

class RangeSelectNode extends SelectNode {
  constructor(exp, context) {
    // const parentContext = context
    super(exp, context)
    const { as } = exp
    // alias is required
    if (isNull(as)) {
      throw parserError(`Missing subquery alias: ${as}`)
    }
    // register identifier against parent context
    this._registerRef(as)
    this._aliasIsUpdatable = false
  }

  _applyAliasToSQL(sql) {
    return `(${sql}) AS ${escapeIdentifier(this.as)}`
  }
}
RangeSelectNode.castable = false

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
    if (this.on.as || this.on._as) {
      throw parserError(`Invalid alias in join condition: ${this.on.as || this.on._as}`)
    }
  }

  _toSQL() {
    return `${this.joinType} JOIN ${this.view.toSQL()} ON ${this.on.toSQL()}`
  }

  _toQL(options) {
    return {
      type: expTypes.JOIN,
      joinType: this.joinType,
      view: this.view.toQL(options),
      on: this.on.toQL(options),
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
    this.view = view
    this._registerRef(as || view, view)
    if (as) {
      this._aliasIsUpdatable = false
    }
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

  _toShort() {
    return {
      name: 'view',
      args: { name: this.view, as: this.as },
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
        this.hasSelectAncestor()
          ? !(isString(view, true) && view in this._context.refs)
            && !(isNull(view) && Object.keys(this._context.refs).length === 1)
          : !(isString(view, true))
      )
    ) {
      throw parserError(`Invalid column expression: ${JSON.stringify(exp)}`)
    }
    if (isNonNull(as) && column === '*') {
      throw parserError(`Invalid column alias: ${as}`)
    }
    if (as === column) {
      this.as = undefined
    }
    this.column = column
    this.view = isNull(view) ? Object.keys(this._context.refs)[0] : (view || undefined)
    // register view + column in context
    this._registerViewColumn(this.view, this.column)
  }

  _toSQL() {
    const column = this.column === '*' ? '*' : escapeIdentifier(this.column)
    return this.view ? `${escapeIdentifier(this.view)}.${column}` : column
  }

  _toQL() {
    return { type: expTypes.COLUMN, column: this.column, view: this.view }
  }

  _toShort() {
    return {
      name: 'column',
      args: { column: this.column, view: this.view, as: this.as, cast: this.cast },
    }
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
    this._registerParam(this.name)
    if (!context.options.parameters) {
      this.value = undefined
      return
    }
    if (!(this.name in context.options.parameters)) {
      throw parserError(`Missing parameter value: ${this.name}`)
    }
    this.value = parseExpression(context.options.parameters[this.name], this._context)
    this.value._validateCastAndAliasLayer(this.cast, this.as)
    this._populateCastAndAliasProxies(this.value)
  }

  _toSQL(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toSQL()
    }
    return `@param('${this.name}')`
  }

  _toQL(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toQL()
    }
    return { type: expTypes.PARAMETER, value: this.name }
  }

  _toShort(options) {
    if (!options.keepParamRefs && this.value !== undefined) {
      return this.value.toShort(options)
    }
    return {
      name: 'param',
      args: { name: this.name, as: this.as, cast: this.cast },
    }
  }
}

class ArrayNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid array syntax: ${JSON.stringify(exp)}`)
    }
    this.values = exp.values.map((e) => {
      const value = parseExpression(e, this._context)
      if (value.as || value._as) {
        throw parserError(`Invalid alias in array expression: ${value.as || value._as}`)
      }
      return value
    })
  }

  _toSQL(options) {
    return `ARRAY[${this.values.map(e => e.toSQL(options)).join(', ')}]`
  }

  _toQL(options) {
    return {
      type: expTypes.ARRAY,
      values: this.values.map(e => e.toQL(options)),
    }
  }

  _toShort(options) {
    return {
      name: 'array',
      args: { values: this.values.map(e => e.toShort(options)), as: this.as, cast: this.cast },
    }
  }
}

class ListNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values)) {
      throw parserError(`Invalid list syntax: ${JSON.stringify(exp)}`)
    }
    this.values = exp.values.map((e) => {
      const value = parseExpression(e, this._context)
      if (value.as || value._as) {
        throw parserError(`Invalid alias in list expression: ${value.as || value._as}`)
      }
      return value
    })
  }

  _toSQL(options) {
    return `(${this.values.map(e => e.toSQL(options)).join(', ')})`
  }

  _toQL(options) {
    return {
      type: expTypes.LIST,
      values: this.values.map(e => e.toQL(options)),
    }
  }

  _toShort(options) {
    return {
      name: 'list',
      args: { values: this.values.map(e => e.toShort(options)), as: this.as, cast: this.cast },
    }
  }
}

class FunctionNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid function syntax: ${JSON.stringify(exp)}`)
    }
    const [name, ...args] = exp.values
    this.name = sanitizeString(name)
    const fn = functions[this.name]
    if (!fn) {
      throw parserError(`Invalid function: ${name}`)
    }
    const { argsLength, minArgsLength, maxArgsLength, defaultCast } = fn
    this.args = (args || []).map((e) => {
      const arg = parseExpression(e, this._context)
      if (arg.as || arg._as) {
        throw parserError(`Invalid alias in function arguments: ${arg.as || arg._as}`)
      }
      return arg
    })
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

  _toSQL(options) {
    return `${this.name}(${this.args.map(e => e.toSQL(options)).join(', ')})`
  }

  _applyCastToSQL(sql) {
    const cast = this.cast || this.defaultCast
    return cast ? `CAST(${sql} AS ${cast})` : sql
  }

  _toQL(options) {
    return {
      type: expTypes.FUNCTION,
      values: [this.name, ...this.args.map(e => e.toQL(options))],
    }
  }

  _toShort(options) {
    return {
      name: 'function',
      args: {
        name: this.name,
        args: this.args.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

class OperatorNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    // binary and left-unary operators
    if (!isArray(exp.values, { minLength: 2 })) {
      throw parserError(`Invalid operator syntax: ${JSON.stringify(exp)}`)
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
    this.operands = operands.map((e) => {
      const operand = parseExpression(e, this._context)
      if (operand.as || operand._as) {
        throw parserError(`Invalid alias in operand: ${operand.as || operand._as}`)
      }
      return operand
    })
  }

  _toSQL(options) {
    if (this.name === 'between' || this.name === 'not between') {
      const [oA, oB, oC] = this.operands
      return `(${oA.toSQL(options)} ${this.name} ${oB.toSQL(options)} AND ${oC.toSQL(options)})`
    }
    return `(${this.operands.map((o, i, all) => {
      const op = i > 0 || all.length === 1 ? `${this.name} ` : ''
      return op + o.toSQL(options)
    }).join(' ')})`
  }

  _toQL(options) {
    return {
      type: expTypes.OPERATOR,
      values: [this.name, ...this.operands.map(e => e.toQL(options))],
    }
  }

  _toShort(options) {
    return {
      name: 'operator',
      args: {
        operator: this.name,
        operands: this.operands.map(e => e.toShort(options)),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

class CastNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (isNull(this.cast) || value === undefined) {
      throw parserError(`Invalid casting syntax: ${JSON.stringify(exp)}`)
    }
    this.value = parseExpression(value, this._context)
    // fold into underlying value if possible
    if (this.value._applyCastAndAliasLayer(this.cast, this.as)) {
      return this.value
    }
  }

  _toSQL(options) {
    return this.value.toSQL(options)
  }

  _toQL(options) {
    return {
      type: expTypes.CAST,
      value: this.value.toQL(options),
    }
  }

  _toShort(options) {
    return {
      name: 'cast',
      args: {
        value: this.value.toShort(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

class SQLNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true)) {
      throw parserError(`Invalid sql syntax: ${JSON.stringify(exp)}`)
    }
    // parse from sql first
    const qlValue = parseSQLExpression(value)
    this.value = parseExpression(qlValue, this._context)
    // fold into underlying value if possible
    if (this.value._applyCastAndAliasLayer(this.cast, this.as)) {
      return this.value
    }
    this._populateCastAndAliasProxies(this.value)
  }

  _toSQL(options) {
    return this.value.toSQL(options)
  }

  _toQL(options) {
    return {
      type: expTypes.CAST,
      value: this.value.toQL(options),
    }
  }

  _toShort(options) {
    return {
      name: 'sql',
      args: {
        sql: this.value.toSQL(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

class ShortNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value } = exp
    if (!isString(value, true)) {
      throw parserError(`Invalid short expression syntax: ${JSON.stringify(exp)}`)
    }
    // parse from short first - returns object expression
    const qlValue = parseShortExpression(value)
    this.value = parseExpression(qlValue, this._context)
    this.short = sanitizeShortExpression(value)
    this.value._validateCastAndAliasLayer(this.cast, this.as)
    this._populateCastAndAliasProxies(this.value)
  }

  _toSQL(options) {
    return (!options.keepParamRefs && this.hasParameterValues()) || !options.keepShorts
      ? this.value.toSQL(options)
      : this.short
  }

  _toQL(options) {
    if ((!options.keepParamRefs && this.hasParameterValues()) || !options.keepShorts) {
      return this.value.toQL(options)
    }
    if (!this.as && !this.cast) {
      return this.short
    }
    return { type: expTypes.SHORT, value: this.short }
  }

  _toShort(options) {
    if (!options.keepParamRefs && this.hasParameterValues()) {
      if (this.as || this.cast) {
        throw parserError('Cannot push cast and alias values into short expression')
      }
      return this.value.toShort(options)
    }
    return this.short
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
    if (this.as || this.cast) {
      return { type: expTypes.PRIMITIVE, value: this.value }
    }
    return this.value
  }

  _toShort(options) {
    return {
      name: 'primitive',
      args: {
        value: this.value.toShort(options),
        as: this.as,
        cast: this.cast,
      },
    }
  }
}

class CaseNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    if (!isArray(exp.values, { minLength: 1 })) {
      throw parserError(`Invalid case syntax: ${JSON.stringify(exp)}`)
    }
    const cases = [...exp.values]
    // first item is either the default result or a cond/res pair
    let defaultRes
    if (!isArray(cases[0])) {
      defaultRes = parseExpression(cases.shift(), this._context)
      if (defaultRes.as || defaultRes._as) {
        throw parserError(`Invalid alias in case expression: ${defaultRes.as || defaultRes._as}`)
      }
    }
    this.defaultRes = defaultRes
    this.cases = cases.map(([cond, res]) => {
      if (isNull(cond) || res === undefined) {
        throw parserError(`Invalid case syntax: ${JSON.stringify(exp)}`)
      }
      return [cond, res].map((e) => {
        const value = parseExpression(e, this._context)
        if (value.as || value._as) {
          throw parserError(`Invalid alias in case expression: ${value.as || value._as}`)
        }
        return value
      })
    })
  }

  _toSQL(options) {
    return `
      CASE
        ${this.case
    .map(([cond, res]) => `WHEN ${cond.toSQL(options)} THEN ${res.toSQL(options)}`)
    .join('\n')}
        ${this.defaultRes ? `ELSE ${this.defaultRes.toSQL(options)}` : ''}
      END
    `
  }

  _toQL(options) {
    const cases = this.cases.map(c => c.map(e => e.toQL(options)))
    return {
      type: expTypes.CASE,
      values: this.defaultRes ? [this.defaultRes.toQL(options), ...cases] : cases,
    }
  }
}

class SortNode extends Node {
  constructor(exp, context) {
    super(exp, context)
    const { value, direction, nulls } = exp
    if (!value || value === true) {
      throw parserError(`Invalid sorting syntax: ${JSON.stringify(exp)}`)
    }
    this.direction = undefined
    if (isNonNull(direction)) {
      const safeDirection = sanitizeString(direction)
      if (!['asc', 'desc'].includes(safeDirection)) {
        throw parserError(`Invalid sorting direction syntax: ${JSON.stringify(exp)}`)
      }
      this.direction = safeDirection
    }
    this.nulls = undefined
    if (isNonNull(nulls)) {
      const safeNulls = sanitizeString(nulls)
      if (!['first', 'last'].includes(safeNulls)) {
        throw parserError(`Invalid sorting nulls syntax: ${JSON.stringify(exp)}`)
      }
      this.nulls = safeNulls
    }
    this.value = parseExpression(value, this._context)
    if (this.value.as || this.value._as) {
      throw parserError(`Invalid alias in sorting expression: ${this.value.as || this.value._as}`)
    }
  }

  _toSQL(options) {
    const direction = this.direction ? ` ${this.direction}` : ''
    const nulls = this.nulls ? ` NULLS ${this.nulls}` : ''
    return this.value.toSQL(options) + direction + nulls
  }

  _toQL(options) {
    return {
      type: expTypes.SORT,
      value: this.value.toQL(options),
      direction: this.direction,
      nulls: this.nulls,
    }
  }
}
SortNode.aliasable = false
SortNode.castable = false

const nodes = {
  SelectNode,
  RangeSelectNode,
  CTESelectNode,
  JoinNode,
  ViewReferenceNode,
  ColumnReferenceNode,
  ParameterReferenceNode,
  SQLNode,
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
objectParsers[expTypes.SELECT_RANGE] = (exp, context) => new RangeSelectNode(exp, context)
objectParsers[expTypes.SELECT_CTE] = (exp, context) => new CTESelectNode(exp, context)
objectParsers[expTypes.JOIN] = (exp, context) => new JoinNode(exp, context)
objectParsers[expTypes.VIEW] = (exp, context) => new ViewReferenceNode(exp, context)
objectParsers[expTypes.COLUMN] = (exp, context) => new ColumnReferenceNode(exp, context)
objectParsers[expTypes.PARAMETER] = (exp, context) => new ParameterReferenceNode(exp, context)
objectParsers[expTypes.SHORT] = (exp, context) => new ShortNode(exp, context)
objectParsers[expTypes.SQL] = (exp, context) => new SQLNode(exp, context)
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
    throw parserError(`Invalid and syntax: ${JSON.stringify(exp)}`)
  }
  return new OperatorNode({ values: ['and', ...values], as, cast }, context)
}
objectParsers[expTypes.OR] = (exp, context) => {
  const { values, as, cast } = exp
  if (!isArray(values)) {
    throw parserError(`Invalid or syntax: ${JSON.stringify(exp)}`)
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
      if (exp.toLowerCase() === 'null') {
        return parseExpression(null, context)
      }
      if (isShortExpression(exp)) {
        return parseExpression({ type: expTypes.SHORT, value: exp }, context)
      }
      // try column
      if (columnRefRE.test(exp)) {
        try {
          const [column, view] = exp.split('.')
          return parseExpression({ type: expTypes.COLUMN, column, view }, context)
        } catch (_) {
          return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)
        }
      }
      return parseExpression({ type: expTypes.PRIMITIVE, value: exp }, context)

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
      throw parserError(`Invalid expression: ${JSON.stringify(exp)}`)
  }
}

module.exports = {
  nodes,
  parseExpression,
}

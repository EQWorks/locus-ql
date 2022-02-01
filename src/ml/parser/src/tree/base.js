/* Base class */
const {
  isString,
  isObjectExpression,
  sanitizeAlias,
  sanitizeCast,
  escapeIdentifier,
  trimSQL,
  parserError,
} = require('../utils')
const { expressionTypes } = require('../types')
const { isShortExpression } = require('../short')


const addToViewColumns = (viewColumns = {}, columnOrColumns) => {
  const columns = isString(columnOrColumns) ? { [columnOrColumns]: true } : columnOrColumns
  if (!('*' in viewColumns || '*' in columns)) {
    return Object.assign(viewColumns, columns)
  }
  return { '*': true }
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
  paramsMustHaveValues: false,
  _safe: true,
}

const parserOptions = {
  keepShorts: true,
  keepParamRefs: false,
  _safe: true,
}

class BaseNode {
  constructor(exp, context) {
    if (!isObjectExpression(exp)) {
      throw parserError(`Expression must be an object: ${JSON.stringify(exp)}`)
    }
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
    // parent context only contains 'options' when root node
    return Object.keys(this._parentContext).length === 1
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
    let ql = this._toQL(safeOptions)
    if (this.constructor.castable && this.cast) {
      if (typeof ql === 'string') {
        ql = { type: expressionTypes.PRIMITIVE, value: ql }
      }
      ql.cast = this.cast
    }
    if (this.constructor.aliasable && this.as) {
      if (typeof ql === 'string') {
        ql = { type: expressionTypes.PRIMITIVE, value: ql }
      }
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
    if (
      !Object.prototype.hasOwnProperty.call(this.constructor, '_parsers')
      || !(name in this.constructor._parsers)
    ) {
      throw parserError(`Parser "${name}" not registered on node type: ${this.constructor.name}`)
    }
    // const memo = `parser:${name}`
    // if (memo in this._memo) {
    //   return this._memo[memo]
    // }
    // this._memo[memo] = this.constructor._parsers[name](this, ...args)
    // if (typeof this._memo[memo] === 'object') {
    //   this._memo[memo] = Object.freeze(this._memo[memo])
    // }
    // return this._memo[memo]
    return this.constructor._parsers[name](this, ...args)
  }

  static registerParser(name, parser) {
    if (!Object.prototype.hasOwnProperty.call(this, '_parsers')) {
      // _parsers cannot be inherited
      this._parsers = {}
    }
    this._parsers[name] = parser
  }
}
BaseNode.aliasable = true
BaseNode.castable = true

module.exports = BaseNode

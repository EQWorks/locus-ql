const { apiError } = require('../util/api-error')


class OneOf {
  constructor(...params) {
    if (params[0] instanceof OneOf) { // ignore others if first is a OneOf instance
      return params[0] // short-circuit no-op return
    }
    this.params = params
  }
  validate(obj) {
    return this.params.some(p => Object.prototype.hasOwnProperty.call(obj, p))
  }
  toString() {
    if (this.params.length > 1) {
      return `one of ${this.params.toString()}`
    }
    return this.params.toString() // ['one'].toString() => 'one'
  }
}
module.exports.oneOf = (...params) => new OneOf(...params)

const isParamMissing = (params, obj, cb = null) => {
  if (!params) return false

  for (const param of params) {
    if (!new OneOf(param).validate(obj)) {
      return cb ? cb(param) : true
    }
  }
  return false
}
const isObject = obj => obj && (typeof obj === 'object')

const isArray = obj => isObject(obj) && (obj instanceof Array)

const _hasParams = target => (...params) => (req, _, next) => {
  if (isObject(params[0]) && !isArray(params[0])) {
    const { required, preferred } = params[0]

    if (isParamMissing(required, req[target])) {
      return next(apiError(`Missing one or more params([${required.join(',')}]) in ${target}`, 400))
    }
    if (isParamMissing(preferred, req[target])) {
      console.log(`Expecting [${preferred.join(',')}] in ${target}`)
    }
    return next()
  }
  isParamMissing(params, req[target], (param) => {
    next(apiError(`Missing ${param.toString()} in ${target}`, 400))
  })
  return next()
}
module.exports.hasQueryParams = _hasParams('query')
module.exports.hasBodyParams = _hasParams('body')

module.exports.accessHasSingleCustomer = (req, _, next) => {
  const { whitelabel, customers } = req.access
  if (
    !Array.isArray(whitelabel)
    || whitelabel.length !== 1
    || !Array.isArray(customers)
    || customers.length !== 1
  ) {
    // eslint-disable-next-line max-len
    return next(apiError('Failed to identify customer; use the `_wl` and `_customer` query parameters'))
  }
  next()
}

module.exports.hasFinAccess = ({ access }, _, next) => {
  const hasFinAccess = [access.whitelabel, access.customers, access.read]
    .every(k => k === -1) && (access.write === -1 || access.write >= 1000)
  if (!hasFinAccess) {
    return next(apiError('No access to finance reports.', 403))
  }
  return next()
}

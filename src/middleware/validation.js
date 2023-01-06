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

const isParamMissing = (params, obj) => {
  if (!params) return false

  for (const param of params) {
    if (!new OneOf(param).validate(obj)) {
      return param
    }
  }
  return ''
}

const _hasParams = target => (...params) => (req, _, next) => {
  if (params[0] && typeof params[0] === 'object') {
    const { required, preferred } = params[0]

    if (isParamMissing(required, req[target])) {
      return next(apiError(`Missing one or more params([${required.join(',')}]) in ${target}`, 400))
    }
    if (isParamMissing(preferred, req[target])) {
      console.log(`Expecting [${preferred.join(',')}] in ${target}`)
    }
    return next()
  }
  const missingParam = isParamMissing(params, req[target])
  if (missingParam) {
    return next(apiError(`Missing ${missingParam} in ${target}`, 400))
  }

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

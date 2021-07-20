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
  show() {
    if (this.params.length > 1) {
      return `one of ${this.params.join(', ')}`
    }
    return this.params[0]
  }
}
module.exports.oneOf = (...params) => new OneOf(...params)

const _hasParams = target => (...params) => (req, _, next) => {
  for (const param of params) {
    if (!new OneOf(param).validate(req[target])) {
      return next(apiError(`Missing ${param.show()} in ${target}`, 400))
    }
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

const { apiError } = require('../util/api-error')


class OneOf {
  constructor(...params) {
    this.params = params
  }
  validate(obj) {
    return this.params.some(p => Object.prototype.hasOwnProperty.call(obj, p))
  }
  show() {
    return `one of ${this.params.join(', ')}`
  }
}
module.exports.oneOf = (...params) => new OneOf(...params)

const _hasParams = target => (...params) => (req, _, next) => {
  for (const param of params) {
    if (param instanceof OneOf && !param.validate(req[target])) {
      return next(apiError(`Missing ${param.show()} in ${target}`, 400))
    }
    if (!req[target][param]) {
      return next(apiError(`Missing '${param}' in ${target}`, 400))
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

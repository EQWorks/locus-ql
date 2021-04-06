const { apiError } = require('../util/api-error')


const _hasParams = target => (...params) => (req, res, next) => {
  for (const param of params) {
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

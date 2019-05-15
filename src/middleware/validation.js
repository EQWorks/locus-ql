const apiError = require('../util/api-error')


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

module.exports.hasQueryParams = (...params) => (req, res, next) => {
  for (const param of params) {
    if (!req.query[param]) {
      const error = new Error(`Missing '${param}' in query string parameters`)
      error.status = 400
      return next(error)
    }
  }
  return next()
}

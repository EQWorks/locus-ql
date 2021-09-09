const Sentry = require('@sentry/node')

const { getContext, hasContext, ERROR_CTX, ERROR_QL_CTX } = require('../util/context')


const { STAGE = 'dev', API_VER, DEBUG, SENTRY_DNS } = process.env
const errorCtx = {
  'App context': ERROR_CTX,
  'QL context': ERROR_QL_CTX,
}

Sentry.init({
  dsn: SENTRY_DNS,
  release: API_VER,
  environment: STAGE,
})

const initRequestContext = Sentry.Handlers.requestHandler({
  request: ['headers', 'method', 'query_string', 'url'],
  serverName: false,
})

// log 1) to console in DEBUG mode, 2) to Sentry otherwise
const logError = (err, req, res, next) => {
  // log to console
  if (['1', 'true'].includes((DEBUG || '').toLowerCase())) {
    console.log(err.originalError || err)
    return next(err)
  }

  // log to sentry when no known status or in 500 series
  if (!err.status || err.status >= 500) {
    return Sentry.withScope((scope) => {
      // append user context
      if (req.access) {
        scope.setUser({ email: req.access.email })
      }
      // append error context
      Object.entries(errorCtx).forEach(([name, path]) => {
        if (hasContext(req, path)) {
          scope.setContext(name, getContext(req, path))
        }
      })
      // append tags
      if (err.tags) {
        scope.setTags(err.tags)
      }
      const sentryHandler = Sentry.Handlers.errorHandler()
      sentryHandler(err.originalError || err, req, res, () => next(err))
    })
  }
  next(err)
}

module.exports = { initRequestContext, logError }

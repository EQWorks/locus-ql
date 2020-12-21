/**
 * Module dependencies.
 */
const serverless = require('serverless-http')

// app
const app = require('./app')

/**
 * Create HTTP server.
 */
module.exports.handler = serverless(app, {
  // attach user access to req
  request: (req, event) => {
    try {
      const { access } = event.requestContext.authorizer
      req.authorizerAccess = JSON.parse(access)
    } catch (err) {
      req.authorizerAccess = undefined
    }
  },
})

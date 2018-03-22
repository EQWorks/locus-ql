/**
 * Module dependencies.
 */
import serverless from 'serverless-http'

// app
import app from './app'

/**
 * Create HTTP server.
 */
module.exports.handler = serverless(app)

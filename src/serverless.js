/**
 * Module dependencies.
 */
const serverless = require('serverless-http')

// app
const app = require('./app')

/**
 * Create HTTP server.
 */
module.exports.handler = serverless(app)

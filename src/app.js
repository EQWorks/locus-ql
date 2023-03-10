const express = require('express')
const logger = require('morgan')
const cors = require('cors')
const compression = require('compression')

const rootRouter = require('./routes')
const { initRequestContext, logError } = require('./middleware/errorlog')


const { API_VER = 'unknown', API_GATEWAY_BASE_PATH = '', IS_OFFLINE = false } = process.env
const STAGE = IS_OFFLINE ? '' : API_GATEWAY_BASE_PATH

const app = express()
// error logging context
app.use(initRequestContext)
// enable cors
// this would enable Access-Control-Allow-Origin: *
app.use(cors())
app.options('*', cors())
// enable gzip
app.use(compression())
// logger
app.use(logger('dev'))
// body parser
app.use(express.json({ limit: '4mb' }))
app.use(express.urlencoded({ extended: false }))

// favicon 404 supression
app.get('/favicon.ico', (_req, res) => res.sendStatus(204))

app.get(`/${STAGE}`, (_req, res) => res.status(200).json({ API_VER, STAGE }))

app.use(`/${STAGE}`, rootRouter)

// error handler for logging
app.use(logError)
// catch-all error handler
// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error(err)
  const { message } = err
  res.return_meta = JSON.stringify({ status: 'failed', message })
  return res.status(err.status || 500).json({ message })
})

module.exports = app

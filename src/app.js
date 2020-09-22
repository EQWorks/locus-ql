const express = require('express')
const logger = require('morgan')
const bodyParser = require('body-parser')
const cors = require('cors')
const compression = require('compression')

const rootRouter = require('./routes/')


const app = express()

const { API_VER = 'unknown', API_GATEWAY_BASE_PATH = '', IS_OFFLINE = false } = process.env
const STAGE = IS_OFFLINE ? '' : API_GATEWAY_BASE_PATH

// enable cors
// this would enable Access-Control-Allow-Origin: *
app.use(cors())
app.options('*', cors())
// enable gzip
app.use(compression())
// logger
app.use(logger('dev'))
// body parser
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))

// favicon 404 supression
app.get('/favicon.ico', (_req, res) => res.sendStatus(204))

app.get(`/${STAGE}`, (_req, res) => res.status(200).json({ API_VER, STAGE }))

app.use(`/${STAGE}`, rootRouter)

// catch-all error handler
// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error(err)
  const { message } = err
  res.return_meta = JSON.stringify({ status: 'falid', message })
  return res.status(err.status || 500).json({ message })
})

module.exports = app

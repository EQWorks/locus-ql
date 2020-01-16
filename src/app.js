const express = require('express')
const logger = require('morgan')
const bodyParser = require('body-parser')
const cors = require('cors')
const compression = require('compression')

const rootRouter = require('./routes/')
const config = require('../config')


const app = express()

const { API_VER = 'unknown' } = process.env

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
app.get('/favicon.ico', (req, res) => res.sendStatus(204))

app.get(`/${config.basePath}`, (_, res) => {
  res.json({ API_VER, STAGE: config.basePath })
})

app.use(`/${config.basePath}`, rootRouter)

// catch-all error handler
// eslint disable otherwise not able to catch errors
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  // set locals, only providing error in development
  console.error(err)
  const { message } = err
  res.status(err.status || 500).json({ message })
})

module.exports = app

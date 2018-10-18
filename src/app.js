const express = require('express')
const path = require('path')
const logger = require('morgan')
const cookieParser = require('cookie-parser')
const bodyParser = require('body-parser')
const cors = require('cors')

const rootRouter = require('./routes/')
const config = require('../config')
// var users = require('./routes/users');

const app = express()

// enable cors
// this would enable Access-Control-Allow-Origin: *
app.use(cors())

app.options('*', cors())

app.get('/favicon.ico', (req, res) => res.sendStatus(204))
app.use(logger('dev'))
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(cookieParser())
app.use(express.static(path.join(__dirname, 'public')))

app.get(`/${config.basePath}`, (_, res) => {
  res.json({
    API_VER: config.commitHash,
    STAGE: config.basePath,
  })
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

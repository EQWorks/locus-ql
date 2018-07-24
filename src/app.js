const express = require('express')
const path = require('path')
const logger = require('morgan')
const cookieParser = require('cookie-parser')
const bodyParser = require('body-parser')
const cors = require('cors')

const api = require('./routes/api')
const config = require('../config')
// var users = require('./routes/users');

const app = express()

// enable cors
// this would enable Access-Control-Allow-Origin: *
app.use(cors())

app.options('*', cors())

// view engine setup
app.set('views', path.join(__dirname, 'views'))
app.set('view engine', 'jade')

// uncomment after placing your favicon in /public
// app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
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

app.use(`/${config.basePath}`, api)

// catch 404 and forward to error handler
app.use((req, res, next) => {
  const err = new Error('ERROR: Path Not Found')
  err.status = 404
  next(err)
})

// error handler
app.use((err, req, res) => {
  // set locals, only providing error in development
  res.locals.message = err.message
  res.locals.error = req.app.get('env') === 'development' ? err : {}

  // render the error page
  res.status(err.status || 500)
  res.render('error')
})

module.exports = app

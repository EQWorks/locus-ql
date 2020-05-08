const express = require('express')
const logger = require('morgan')
const bodyParser = require('body-parser')
const cors = require('cors')
const compression = require('compression')

const rootRouter = require('./routes/')
const { pool } = require('./util/db')


const app = express()

const { API_VER = 'unknown', API_GATEWAY_BASE_PATH: STAGE = '' } = process.env

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

app.get(`/${STAGE}`, (_, res) => {
  res.json({ API_VER, STAGE })
})

app.use(`/${STAGE}`, rootRouter)

// pass API response into log

app.use((req, res, next) => {
  const {
    id,
    return_code,
    return_meta,
    res_info,
  } = req.log

  // If no log id what error should be return
  if (!id) {
    return res.json({ message: 'No log recorded' })
  }

  pool.query(
    `
      UPDATE locus_log
      SET return_code = $1, return_meta = $2
      WHERE id = $3
    `,
    [return_code, return_meta, id],
  )
    .catch(next)
  return res.status(return_code).json(res_info)
})

// catch-all error handler
// eslint disable otherwise not able to catch errors
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  console.error(err)
  const { message } = err
  // if has auditlog, write the error message in locuslog table
  if (!req.log || Object.entries(req.log).length === 0) {
    return res.status(err.status || 500).json({ message })
  }

  if (req.log && Object.entries(req.log).length !== 0) {
    const { id } = req.log
    const return_code = err.status || 500
    const return_meta = JSON.stringify({
      status: 'falid',
      message: `${message}`,
    })
    pool.query({
      text: `
        UPDATE locus_log
        SET return_code = $1, return_meta = $2
        WHERE id = $3
      `,
      values: [return_code, return_meta, id],
    })
      .then(() => res.status(err.status || 500).send({ message }))
      .catch(console.error)
  }
})

module.exports = app

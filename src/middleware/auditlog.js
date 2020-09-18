const onFinished = require('on-finished')

const { pool } = require('../util/db')


const insertLog = (...values) => (_, res) => {
  // local "STAGE"-less
  if (!process.env.API_GATEWAY_BASE_PATH) {
    return console.log('Local Audit Log:', ...values)
  }
  // remote deployment
  return pool.connect()
    .then(client => client.query({
      text: `
        INSERT INTO locus_log(
          email,
          time_st,
          action,
          payload,
          http_method,
          api_path,
          return_code,
          return_meta
        )
        VALUES ($1, now(), $2, $3, $4, $5, $6, $7);
      `,
      values: [...values, res.statusCode, res.return_meta],
    }).finally(() => client.release())) // the finally is intentionally in client scope
    .catch(console.error)
}

module.exports.auditlog = (action = 'others') => (req, res, next) => {
  const { access: { email }, method, originalUrl } = req
  const anyBodyParams = Object.entries(req.body).length
  const payload = (!anyBodyParams) ? req.query : req.body

  onFinished(res, insertLog(email, action, JSON.stringify(payload), method, originalUrl))
  next()
}

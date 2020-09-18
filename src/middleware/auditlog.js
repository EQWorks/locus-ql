const onFinished = require('on-finished')

const { pool } = require('../util/db')


const insertLog = (...values) => (_, res) => {
  // local "STAGE"-less
  if (!process.env.API_GATEWAY_BASE_PATH) {
    return console.log('Local Audit Log:', ...values, res.statusCode, res.return_meta)
  }
  // remote deployment
  function insert() {
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
      }).then(() => client.release())) // intentionally in client scope
  }
  let p = Promise.reject() // promise retry hack https://stackoverflow.com/a/38225011/158111
  for (let i = 0; i < 3; i++) { // max 3 retries
    p = p.catch(insert).then(() => null)
  }
  return p.catch(console.error)
}

module.exports.auditlog = (action = 'others') => (req, res, next) => {
  const { access: { email }, method, originalUrl } = req
  const anyBodyParams = Object.entries(req.body).length
  const payload = (!anyBodyParams) ? req.query : req.body

  onFinished(res, insertLog(email, action, JSON.stringify(payload), method, originalUrl))
  next()
}

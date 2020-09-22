const { pool } = require('../util/db')


const insertLog = (props) => {
  const {
    action, // custom
    access: { email }, method, originalUrl, body, query, // req {}
    statusCode, return_meta = {}, // res {}
  } = props
  const payload = JSON.stringify(!Object.keys(body).length ? query : body)
  // local context (serverless-less, yarn start)
  if (!process.env.API_GATEWAY_BASE_PATH) {
    console.log('Local Audit Log:\n')
    return console.log(props)
  }
  // serverless context (remote or yarn offline), async/await for readability
  async function insert() {
    const client = await pool.connect()
    await client.query({
      text: `
          INSERT INTO locus_log(
            email,
            action,
            payload,
            http_method,
            api_path,
            return_code,
            return_meta,
            time_st
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, now());
        `,
      values: [email, action, payload, method, originalUrl, statusCode, return_meta],
    })
    return client.release()
  }
  let p = Promise.reject() // promise retry hack https://stackoverflow.com/a/38225011/158111
  for (let i = 0; i < 3; i++) { // max 3 retries
    p = p.catch(insert)
  }
  return p.catch(console.error)
}

module.exports.auditlog = (action = 'others') => (req, res, next) => {
  res.once('finish', () => {
    insertLog({ action, ...req, ...res })
  })
  return next()
}

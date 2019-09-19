const { pool } = require('../util/db')


module.exports.auditlog = action => (req, res, next) => {
  const { access: { email }, body, method, originalUrl } = req
  pool.query(
    `
      INSERT INTO locus_log(email, time_st, action, payload, http_method, api_path)
      VALUES($1, now(), $2, $3, $4, $5)
      RETURNING id
    `,
    [email, action, JSON.stringify(body), method, originalUrl],
  )
    .then(() => next())
    .catch(next)
}

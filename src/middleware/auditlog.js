const { pool } = require('../util/db')

/**
 * log need to have four params
 * return_code as statusCode
 * return_meta for locus_log table to record return message
 * res_info for response to snoke
 * logID
 *
 * logID will be generate from here
 * and other params need to be defined in each endpoint which used log middleware
 */

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
    .then((res) => {
      const { rows: [{ id }] } = res
<<<<<<< HEAD
<<<<<<< HEAD
      req.log = { logID: id }
=======
      req.logID = id
>>>>>>> log - pass logid down to endpoint
=======
      req.log = { logID: id }
>>>>>>> log - write response into log
      next()
    })
    .catch(next)
}

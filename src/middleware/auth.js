const jwt = require('jsonwebtoken')
const axios = require('axios')

const config = require('../../config')


const KEY_WARDEN_HOST = config.keyWarden.host
const KEY_WARDEN_STAGE = config.keyWarden.stage
const KEY_WARDEN_BASE = `${KEY_WARDEN_HOST}/${KEY_WARDEN_STAGE}`

function jwtMiddleware(req, res, next) {

  // quick validation
  if (!(req.headers && req.headers['x-firstorder-token'] && req.headers['x-firstorder-token'].length > 0))
    res.sendStatus(401)

  const uJWT = req.headers['x-firstorder-token']
  const decoded = jwt.decode(uJWT)
  // console.log('token payload middleware', decoded)

  let access = {}
  // default whitelabel
  access.whitelabel = 0
  if (decoded.api_access && decoded.api_access.wl)
    access.whitelabel = decoded.api_access.wl

  // default customers
  access.customers = 0
  if (decoded.api_access && decoded.api_access.customers)
    access.customers = decoded.api_access.customers

  access.email = ''
  if (decoded.email)
    access.email = decoded.email

  req.access = access

  axios({
    url: `${KEY_WARDEN_BASE}/confirm`,
    method: 'get',
    headers: {
      'eq-api-jwt': uJWT
    },
    params: {
      light: 1
    },
  })
  .then(data => {
    next()
  })
  .catch(error => {
    if (error.response)
      console.log('error', error.response.data)
    res.sendStatus(401)
  })

}


module.exports = {
  jwt: jwtMiddleware
};

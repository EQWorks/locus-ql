const jwt = require('jsonwebtoken')
const axios = require('axios')
const pg = require('pg')

const config = require('../../config')


const pool = new pg.Pool(config.pg)
// the pool with emit an error on behalf of any idle clients
// it contains if a backend error or network partition happens
pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err)
  process.exit(-1)
})

const KEY_WARDEN_HOST = config.keyWarden.host
const KEY_WARDEN_STAGE = config.keyWarden.stage
const KEY_WARDEN_BASE = `${KEY_WARDEN_HOST}/${KEY_WARDEN_STAGE}`

function jwtMiddleware(req, res, next) {
  // quick validation
  if (!(
    req.headers &&
    req.headers['x-firstorder-token'] &&
    req.headers['x-firstorder-token'].length > 0
  )) {
    res.sendStatus(401)
    return
  }

  const uJWT = req.headers['x-firstorder-token']
  const decoded = jwt.decode(uJWT)
  // console.log('token payload middleware', decoded)

  const access = {}
  // default whitelabel
  access.whitelabel = 0
  if (decoded.api_access && decoded.api_access.wl) { access.whitelabel = decoded.api_access.wl }

  // default customers
  access.customers = 0
  if (decoded.api_access && decoded.api_access.customers) {
    access.customers = decoded.api_access.customers
  }

  access.email = ''
  if (decoded.email) { access.email = decoded.email }

  req.access = access

  axios({
    url: `${KEY_WARDEN_BASE}/confirm`,
    method: 'get',
    headers: { 'eq-api-jwt': uJWT },
    params: { light: 1 },
  })
    .then(() => {
      next()
    })
    .catch((error) => {
      if (error.response) { console.log('error', error.response.data) }
      res.sendStatus(401)
    })
}

const haveLayerAccess = async (access, layerID) => {
  const wl = access.whitelabel
  const cu = access.customers
  const { email } = access

  let where = 'WHERE layer_id = $1'
  let values = [layerID]

  if (Array.isArray(wl) && wl.length > 0 && cu === -1) {
    where += " AND whitelabel = ANY ($2) AND account in ('0', '-1'))"
    values = [...values, wl]
  } else if (Array.isArray(wl) && wl.length > 0 && Array.isArray(cu) && cu.length > 0) {
    where += " AND whitelabel = ANY ($2) AND customer = ANY ($3) AND account in ('0', '-1', $4))"
    values = [...values, wl, cu, email]
  } else if (!(wl === -1 && cu === -1)) {
    return false
  }

  try {
    const result = await pool.query(
      `
      SELECT *
      FROM layer
      ${where}
      `,
      values,
    )
    return result.rows.length === 1
  } catch (error) {
    console.log(error)
    return false
  }
}

// assumes req.access exist
const layerAuth = async (req, res, next) => {
  const layerAccess = await haveLayerAccess(req.access, req.params.id)
  if (layerAccess) {
    next()
  } else {
    res.status(403).json('Access to layer not allowed')
  }
}

module.exports = {
  jwt: jwtMiddleware,
  layer: layerAuth,
}

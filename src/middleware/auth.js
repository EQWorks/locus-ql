const jwt = require('jsonwebtoken')
const axios = require('axios')
const { get } = require('lodash')

const { keyWarden: { host: KEY_WARDEN_HOST, stage: KEY_WARDEN_STAGE } } = require('../../config')
const apiError = require('../util/api-error')
const { pool } = require('../util/db')


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
  if (decoded.api_access && decoded.api_access.wl) {
    access.whitelabel = decoded.api_access.wl
  }

  // default customers
  access.customers = 0
  if (decoded.api_access && decoded.api_access.customers) {
    access.customers = decoded.api_access.customers
  }

  access.email = ''
  if (decoded.email) {
    access.email = decoded.email
  }

  access.write = 0
  if (decoded.api_access && decoded.api_access.write) {
    access.write = decoded.api_access.write
  }

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
      if (error.response) {
        console.log('error', error.response.data)
      }
      res.sendStatus(401)
    })
}

const haveLayerAccess = async (req, layerIDs) => {
  const { whitelabel: wl, customers: cu, email } = req.access
  let where = 'WHERE layer_id = ANY ($1)'
  let values = [layerIDs]

  try {
    if (Array.isArray(wl) && wl.length > 0 && cu === -1) {
      where += ' AND (whitelabel = -1 OR (whitelabel = ANY ($2) AND account in (\'0\', \'-1\')))'
      values = [...values, wl]
    } else if (Array.isArray(wl) && wl.length > 0 && Array.isArray(cu) && cu.length > 0) {
      // get subscribe layers
      const { rows } = await pool.query(`
        SELECT type_id
        FROM market_ownership_flat MO
        LEFT JOIN customers CU on CU.customerid = MO.customer
        WHERE MO.type = 'layer' AND MO.whitelabel = ${wl[0]} AND CU.agencyid = ${cu[0]}
      `)
      const subscribeLayerIDs = rows.map(layer => layer.type_id)

      where += `AND
        (
          whitelabel = -1
          OR
          (whitelabel = ANY ($2) AND customer = ANY ($3) AND account in ('0', '-1', $4))
          OR
          layer_id = ANY ($5)
        )
      `
      values = [...values, wl, cu, email, subscribeLayerIDs]
    } else if (!(wl === -1 && cu === -1)) {
      return false
    }


    const { rows: layers } = await pool.query(
      `
      SELECT *
      FROM layer
      ${where}
      `,
      values,
    )
    req.layers = layers
    return layers.length === layerIDs.length
  } catch (error) {
    console.log(error)
    return false
  }
}

// assumes req.access exist
// pathToID should lead to either a layerID or an array of layerID
const layerAuth = (pathToID = 'params.id') => async (req, res, next) => {
  const layer = get(req, pathToID)
  const layers = Array.isArray(layer) ? layer : [layer]
  const layerAccess = await haveLayerAccess(req, layers)
  if (layerAccess) {
    next()
  } else {
    return next(apiError('Access to layer not allowed', 403))
  }
}

const internalAuth = (req, res, next) => {
  const { whitelabel, customers } = req.access
  if (whitelabel === -1 && customers === -1) {
    next()
  } else {
    return next(apiError('Only internal are allowed', 403))
  }
}

const devAuth = ({ access }, res, next) => {
  if (['whitelabel', 'customers', 'write'].every(key => access[key] === -1)) {
    return next()
  }
  return next(apiError('Only devs are allowed', 403))
}

const mapAuth = (pathToID = 'params.id') => async (req, res, next) => {
  const { whitelabel: wl, customers: cu, email } = req.access
  let where = 'WHERE map_id = $1'
  let values = [get(req, pathToID)]

  if (Array.isArray(wl) && wl.length > 0 && cu === -1) {
    where += ' AND (whitelabel = -1 OR (whitelabel = ANY ($2) AND account in (\'0\', \'-1\')))'
    values = [...values, wl]
  } else if (Array.isArray(wl) && wl.length > 0 && Array.isArray(cu) && cu.length > 0) {
    // eslint-disable-next-line max-len
    where += ' AND (whitelabel = -1 OR (whitelabel = ANY ($2) AND customer = ANY ($3) AND account in (\'0\', \'-1\', $4)))'
    values = [...values, wl, cu, email]
  } else if (!(wl === -1 && cu === -1)) {
    return false
  }

  try {
    const { rows: maps } = await pool.query(
      `
      SELECT *
      FROM map
      ${where}
      `,
      values,
    )
    req.map = maps[0]

    if (maps.length === 1) {
      return next()
    }

    return next(apiError('Access to map not allowed', 403))
  } catch (error) {
    console.error(error)
    return next(apiError(error, 400))
  }
}

const hasWrite = requiredWrite => ({ access: { write } }, res, next) => {
  if (write === -1 || write >= requiredWrite) {
    next()
  } else {
    return next(apiError('Insufficient write access', 403))
  }
}

module.exports = {
  jwt: jwtMiddleware,
  layer: layerAuth,
  internal: internalAuth,
  dev: devAuth,
  map: mapAuth,
  write: hasWrite,
}

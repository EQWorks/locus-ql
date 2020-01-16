const jwt = require('jsonwebtoken')
const axios = require('axios')
const { get } = require('lodash')

const apiError = require('../util/api-error')
const { pool } = require('../util/db')


const { KEY_WARDEN_HOST, KEY_WARDEN_STAGE } = process.env
const KEY_WARDEN_BASE = `${KEY_WARDEN_HOST}/${KEY_WARDEN_STAGE}`


function jwtMiddleware(req, _, next) {
  // DEPRECATED `x-firstorder-token`
  const uJWT = req.get('eq-api-jwt') || req.get('x-firstorder-token')

  // quick validation
  if (!(req.headers && uJWT && uJWT.length > 0)) {
    return next(apiError('Invalid JWT', 401))
  }

  const {
    email = '',
    api_access = {},
    prefix,
  } = jwt.decode(uJWT)

  const { write = 0, read = 0 } = api_access
  let { wl: whitelabel = 0, customers = 0 } = api_access

  // both are integers
  const { _wl, _customer } = req.query
  const wlID = parseInt(_wl)
  const cuID = parseInt(_customer)
  // validate _wl and _customer
  if (_wl && wlID && (!_customer || cuID)) {
    const isInternal = whitelabel === -1 && customers === -1
    if (isInternal || whitelabel.includes(_wl)) {
      whitelabel = [wlID]
    }

    if (_customer && (isInternal ||
      (whitelabel.includes(wlID) && (customers === -1 || customers.includes(cuID)))
    )) {
      customers = [cuID]
    }
  }

  req.access = { whitelabel, customers, write, read, email, prefix }

  axios({
    url: `${KEY_WARDEN_BASE}/confirm`,
    method: 'get',
    headers: { 'eq-api-jwt': uJWT },
    params: { light: 1 },
  }).then(() => next()).catch(next)
}

const haveLayerAccess = async (req, layerIDs) => {
  const { whitelabel: wl, customers: cu, email } = req.access
  let where = 'WHERE layer_id = ANY ($1)'
  let values = [layerIDs]
  let join = ''
  try {
    if (Array.isArray(wl) && wl.length > 0 && cu === -1) {
      where += ' AND (whitelabel = -1 OR (whitelabel = ANY ($2) AND account in (\'0\', \'-1\')))'
      values = [...values, wl]
    } else if (Array.isArray(wl) && wl.length > 0 && Array.isArray(cu) && cu.length > 0) {
      // get subscribe layers
      const { rows } = await pool.query(`
        SELECT type_id
        FROM market_ownership_flat MO
        WHERE MO.type = 'layer' AND MO.whitelabel = ${wl[0]} AND MO.customer = ${cu[0]}
      `)
      const subscribeLayerIDs = rows.map(layer => layer.type_id)
      join = 'LEFT JOIN customers as CU ON CU.customerid = layer.customer'
      where += ` AND
        (
          whitelabel = -1
          OR
          (
            whitelabel = ANY ($2)
            AND (customer = ANY ($3) OR agencyid = ANY ($3))
            AND account in ('0', '-1', $4)
          )
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
      ${join}
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
const layerAuth = (pathToID = 'params.id', pathToSecondaryID = false) => async (req, res, next) => {
  try {
    const layer = get(req, pathToID)
    const layers = Array.isArray(layer) ? layer : [layer]
    if (pathToSecondaryID) {
      const layer2 = get(req, pathToSecondaryID)
      if (Array.isArray(layer2)) {
        layers.push(...layer2)
      } else {
        layers.push(layer2)
      }
    }
    const layerAccess = await haveLayerAccess(req, layers)
    if (layerAccess) {
      return next()
    }
    return next(apiError('Access to layer not allowed', 403))
  } catch (error) {
    return next(apiError('Invalid layer format', 403))
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

const dataProviderAuth = (req, res, next) => {
  const { whitelabel, customers, read, write } = req.access
  if (whitelabel === -1 && customers === -1) {
    return next()
  } else if (whitelabel !== -1 && customers === -1 && read === 500 && write === 10) {
    return next()
  }
  return next(apiError('Only data providers are allowed', 403))
}

const devAuth = ({ access }, res, next) => {
  if (access.prefix !== 'dev') {
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


// assumes req.access exist
const whitelabelAuth = (pathToID = 'query.wlID') => (req, res, next) => {
  try {
    const wlID = parseInt(get(req, pathToID))

    const internal = req.access.whitelabel === -1 && req.access.customers === -1
    if (!internal && !req.access.whitelabel.includes(wlID)) {
      return next(apiError('Access to whitelabel not allowed', 403))
    }
    return next()
  } catch (error) {
    return next(apiError('Invalid wlID format', 403))
  }
}

module.exports = {
  jwt: jwtMiddleware,
  layer: layerAuth,
  internal: internalAuth,
  dataProvider: dataProviderAuth,
  dev: devAuth,
  map: mapAuth,
  write: hasWrite,
  whitelabel: whitelabelAuth,
}

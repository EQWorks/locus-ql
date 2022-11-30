const axios = require('axios')
const { get } = require('lodash')

const { useAPIErrorOptions } = require('../util/api-error')
const { pool } = require('../util/db')
const { queryWithCache, pgWithCache } = require('../util/cache')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'auth' } })
const { KEY_WARDEN_HOST, KEY_WARDEN_STAGE } = process.env
const KEY_WARDEN_BASE = `${KEY_WARDEN_HOST}/${KEY_WARDEN_STAGE}`

// sets req.access
const loadUserAccess = async (req, _, next) => {
  try {
    // DEPRECATED `x-firstorder-token`
    const token = req.get('eq-api-jwt') || req.get('x-firstorder-token')

    // quick validation
    if (!(req.headers && token && token.length > 0)) {
      throw apiError('Invalid JWT', 401)
    }

    let {
      email = '',
      api_access = {},
      prefix,
      product: tokenProduct,
    } = req.authorizerAccess || {}

    // it is the route's responsibility to validate whether or not it accepts product <> locus
    const _product = req.get('X-EQ-Product')
    const product = _product && ['atom', 'locus'].includes(_product) ? _product : 'locus'

    // if bypassed the lambda authorizer (e.g. local), validate token
    if (!req.authorizerAccess) {
      // cache access for 5 minutes (same as authorizer)
      const confirmedAccess = await queryWithCache(
        ['user-access', token],
        () => axios({
          url: `${KEY_WARDEN_BASE}/confirm`,
          method: 'get',
          headers: { 'eq-api-jwt': token },
          params: { product },
        }).then(res => res.data),
        { ttl: 900, maxAge: 900, gzip: false },
      )
      email = confirmedAccess.user
      api_access = confirmedAccess.access
      prefix = confirmedAccess.access.prefix
      tokenProduct = confirmedAccess.product
    }

    if (tokenProduct !== product) {
      throw apiError('Invalid JWT', 401)
    }

    const { write = 0, read = 0, version, policies } = api_access
    const { wl: whitelabel = [], customers = [] } = api_access

    // payer wl/cu
    // default is whitelabel[0], customers[0]
    const payer = {
      whitelabel: whitelabel === -1 ? -1 : whitelabel[0],
      customer: customers === -1 ? -1 : customers[0],
    }

    // append access to req
    req.access = {
      whitelabel,
      customers,
      write,
      read,
      email,
      prefix,
      token,
      product,
      payer,
      version,
      ...(policies ? { policies } : {}),
    }

    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to authenticate user', 401))
  }
}

// parse string
// validate int > 0
// check that in allowed values
const sanitizeIdParam = (name, value, allowedValues) => {
  if (!value) {
    return
  }
  const safeValue = parseInt(value)
  if (Number.isNaN(safeValue) || safeValue <= 0) {
    throw apiError(`Invalid value for ${name}: ${value}`)
  }
  if (allowedValues && allowedValues !== -1 && !allowedValues.includes(safeValue)) {
    throw apiError('Unauthorized', 403)
  }
  return safeValue
}

// confirm wl/cu affiliation and return agency id
const getCustomerAgencyID = async (whitelabelID, customerID) => {
  const [{ agencyID } = {}] = await pgWithCache(
    `
      SELECT
        CASE agencyid
          WHEN 0 THEN customerid
          ELSE agencyid
        END AS "agencyID"
      FROM public.customers
      WHERE
        whitelabelid = $1
        AND customerid = $2
        AND isactive
    `,
    [whitelabelID, customerID],
    pool,
    { maxAge: 86400 }, // 1 day
  )
  if (!agencyID) {
    throw apiError('Invalid whitelabel and/or customer', 403)
  }
  return agencyID
}

// sets whitelabel, customers and payer
const scopeUserAccess = async (req, _, next) => {
  try {
    const { whitelabel, customers } = req.access
    const { _wl, _customer, payingwl, payingcu } = req.query

    // set whitelabel and customers access
    const safeWl = sanitizeIdParam('_wl', _wl, whitelabel)
    if (safeWl) {
      req.access.whitelabel = [safeWl]
      const safeCustomer = sanitizeIdParam('_customer', _customer, customers)
      if (safeCustomer) {
        // check affiliation
        const agencyID = await getCustomerAgencyID(safeWl, safeCustomer)
        req.access.customers = [agencyID]
      }
    }

    // set payer
    const safePayingWl = sanitizeIdParam('payingwl', payingwl, whitelabel)
    if (safePayingWl) {
      req.access.payer.whitelabel = safePayingWl
      const safePayingCustomer = sanitizeIdParam('payingcu', payingcu, customers)
      if (safePayingCustomer) {
        // check affiliation
        const payingAgencyID = await getCustomerAgencyID(safePayingWl, safePayingCustomer)
        req.access.payer.customer = payingAgencyID
      }
    }

    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to set the customer and/or the payer'))
  }
}

const haveLayerAccess = async ({ wl, cu, layerIDs }) => {
  // TODO: make moduler (e.g. subscribed, owned, by type etc.)
  // TODO: is email necessary?
  // AND account in ('0', '-1', $4)
  try {
    const values = [layerIDs]
    const subscribed = []
    const access = []

    // set filters
    if (wl !== -1 && wl.length && (cu === -1 || cu.length)) {
      values.push(wl)
      subscribed.push(`MO.whitelabel = ANY ($${values.length})`)
      access.push(`whitelabel = ANY ($${values.length})`, "account in ('0', '-1')")
      if (cu !== -1) {
        values.push(cu)
        subscribed
          .push(`(MO.customer = ANY ($${values.length}) \
          OR CU.agencyid = ANY ($${values.length}))`)
        access
          .push(`(layer.customer = ANY ($${values.length}) \
          OR CU.agencyid = ANY ($${values.length}))`)
      }
    }

    const { rows } = await pool.query(`
      ${subscribed.length ? `
        WITH subscribed_layers AS (
          SELECT type_id
          FROM market_ownership_flat MO
          LEFT JOIN customers as CU ON CU.customerid = MO.customer
          WHERE
            MO.type = 'layer'
            AND ${subscribed.join(' AND ')}
        )
      ` : ''}
      SELECT *
      FROM layer
      ${cu !== -1 && cu.length ? 'LEFT JOIN customers as CU ON CU.customerid = layer.customer' : ''}
      WHERE
        layer.layer_id = ANY($1)
        AND (
          ${wl === -1 ? 'TRUE' : 'layer.whitelabel = -1'}
          ${access.length ? `OR (${access.join(' AND ')})` : ''}
          ${subscribed.length ? 'OR layer.layer_id = ANY (SELECT * FROM subscribed_layers)' : ''}
        )
    `, values)
    return rows
  } catch (err) {
    console.log(err)
    return []
  }
}

const validateAccess = ({
  authorizedPrefix = [],
  unAuthorizedPrefix = [],
} = {}) => (req, _, next) => {
  const { prefix } = req.access

  if (authorizedPrefix.length) {
    if (!authorizedPrefix.includes(prefix)) {
      return next(apiError('Not authorized to access', 403))
    }
    return next()
  }

  if (unAuthorizedPrefix.length) {
    if (unAuthorizedPrefix.includes(prefix)) {
      return next(apiError('Not authorized to access', 403))
    }
    return next()
  }
  next()
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
    const { whitelabel: wl, customers: cu } = req.access
    const layerAccess = await haveLayerAccess({ wl, cu, layerIDs: layers })
    if (layerAccess && layerAccess.length) {
      // we have layers
      req.layers = layerAccess
      return next()
    }
    return next(apiError('Access to layer not allowed', 403))
  } catch (error) {
    return next(apiError('Invalid layer format', 403))
  }
}

const isInternal = (req, res, next) => {
  const { whitelabel, customers } = req.access
  if (whitelabel === -1 && customers === -1) {
    return next()
  }
  return next(apiError('Only internal are allowed', 403))
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

const isDev = ({ access: { prefix, whitelabel, customers } = {} }, _, next) => {
  if (prefix === 'dev' && whitelabel === -1 && customers === -1) {
    return next()
  }
  return next(apiError('Only devs are allowed', 403))
}

const isAppReviewer = ({ access: { prefix } = {} }, _, next) => {
  if (prefix === 'appreviewer') {
    return next()
  }
  return next(apiError('Only appreviewers are allowed', 403))
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
    next(getSetAPIError(error, 'Failed to access map'))
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

const hhSegmentAuth = (req, res, next) => {
  // Accepted whitelabels are internal, OPTA and ONLIA (under OPTA)
  const acceptedWhitelabels = [1202, 1367]

  if (req.access.whitelabel === -1 ||
      req.access.whitelabel.some(wl => acceptedWhitelabels.includes(wl))) {
    return next()
  }

  return next(apiError('Access not allowed', 403))
}

const hubAuth = ({ requireWrite = false }) => (req, _, next) => {
  const { whitelabel, customers, prefix, version, policies = [] } = req.access
  const internal = whitelabel === -1 && customers === -1
  const prefixes = ['dev', 'tester']
  const byPrefix = prefixes.includes(prefix)
  let pass = false
  const policy = version && policies.find(p => p.match(/^hub/))
  if (policy) {
    const [, read, write] = policy.split(':')
    pass = (!requireWrite && read) || write
  }
  if (internal || byPrefix || pass) {
    return next()
  }
  return next(apiError(`Only internal or one of ${prefixes.toString()} are allowed`, 403))
}

const includeMobileSDK = validateAccess({ authorizedPrefix: ['mobilesdk', 'dev'] })

const excludeMobileSDK = validateAccess({ unAuthorizedPrefix: ['mobilesdk'] })

module.exports = {
  loadUserAccess,
  scopeUserAccess,
  layer: layerAuth,
  isInternal,
  dataProvider: dataProviderAuth,
  isDev,
  isAppReviewer,
  map: mapAuth,
  write: hasWrite,
  whitelabel: whitelabelAuth,
  layerAccess: haveLayerAccess,
  hhSegments: hhSegmentAuth,
  hub: hubAuth,
  includeMobileSDK,
  excludeMobileSDK,
}

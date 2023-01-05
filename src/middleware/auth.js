const axios = require('axios')

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


module.exports = {
  loadUserAccess,
  scopeUserAccess,
}

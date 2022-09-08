const { useAPIErrorOptions } = require('../util/api-error')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'policies' } })

// categorize policies by prefix
const parsePolicies = policies => policies.reduce((acc, policy) => {
  const splitPolicy = (p, obj) => {
    const [prefix, ...rest] = p.split(':')
    if (obj[prefix]) {
      obj[prefix].push(rest.join(':'))
    } else {
      obj[prefix] = [rest.join(':')]
    }
  }

  if (Array.isArray(policy)) {
    const obj = {}
    for (const p of policy) {
      splitPolicy(p, obj)
    }
    const key = `one-of-${Object.keys(obj).join('-')}`
    acc[key] = obj
  } else {
    splitPolicy(policy, acc)
  }
  return acc
}, {})

// match policies
const policyCompare = (policy, targetPolicy) => {
  const target = targetPolicy.match(/\*/)
    ? `^${targetPolicy.replace(/\*/, '.*')}$`
    : targetPolicy
  const regex = new RegExp(target)
  return regex.test(policy)
}

// check if user policies match at least 1 required policies for a particular prefix
const checkRequiredPolicies = (user, target) => (
  user.some(p => target.some(tp => policyCompare(p, tp)))
)

const confirmAccess = (...access) => (req, _, next) => {
  try {
    const { policies } = req.access

    if (!policies || (policies && !policies.length)) {
      throw apiError('Failed to confirm access, missing user policies.', 400)
    }
    if (!access.length) {
      throw apiError('Failed to confirm access, missing policy rules.', 400)
    }

    const userPolicies = parsePolicies(policies)
    const targetPolicies = parsePolicies(access)
    for (const [prefix, tp] of Object.entries(targetPolicies)) {
      // high-level check if user has all required policy categories
      if (prefix.startsWith('one-of-')) {
        const allPrefixes = Object.keys(tp)
        const found = Object.keys(userPolicies).filter(prefix => allPrefixes.includes(prefix))
        const pass = found.some(prefix => checkRequiredPolicies(userPolicies[prefix], tp[prefix]))
        if (!pass) {
          throw apiError(`Invalid access, should have one of ${
            allPrefixes.join(', ')} policies`, 403)
        }
      } else {
        if (!userPolicies[prefix]) {
          throw apiError(`Invalid access, missing required policies for ${prefix}`, 403)
        }
        const pass = checkRequiredPolicies(userPolicies[prefix], tp)
        if (!pass) {
          throw apiError(`Invalid access, missing required policies for ${prefix}`, 403)
        }
      }
    }

    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to validate access', 500))
  }
}

// TODO: take prefixes as an argument to make this more flexible
const confirmAccessRoleCheck = (...access) => (req, _, next) => {
  const { prefix, version } = req.access
  const prefixes = ['dev']
  const byPrefix = prefixes.includes(prefix)
  // allow access if it is v0 user for backward compatibility
  // dev is not restricted by policy
  if (!version || byPrefix) {
    return next()
  }
  confirmAccess(...access)(req, _, next)
}

module.exports = { confirmAccess, confirmAccessRoleCheck }

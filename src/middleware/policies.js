const { useAPIErrorOptions } = require('../util/api-error')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'policies' } })

// categorize policies by prefix
const parsePolicies = policies => (policies.reduce((acc, p) => {
  const [prefix, ...rest] = p.split(':')
  if (acc[prefix]) {
    acc[prefix].push(rest.join(':'))
  } else {
    acc[prefix] = [rest.join(':')]
  }
  return acc
}, {}))

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
      if (!userPolicies[prefix]) {
        throw apiError(`Invalid access, missing required policies for ${prefix}`, 403)
      }
      const pass = checkRequiredPolicies(userPolicies[prefix], tp)
      if (!pass) {
        throw apiError(`Invalid access, missing required policies for ${prefix}`, 403)
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

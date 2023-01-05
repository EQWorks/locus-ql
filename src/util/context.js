// use WeakMap so requests can be garbage collected
const context = new WeakMap()

/**
 * Returns request-scoped context at local path (or root if not specified)
 * A context object will be created if it does not exist
 * @param {Express.Request} req Express request
 * @param {string[]} [path] Path to local context (e.g. service context)
 * @returns {Object} Context object
 */
const getContext = (req, path) => {
  // get/set req context
  const ctx = context.get(req) || context.set(req, {}).get(req)
  if (!path) {
    return ctx
  }
  // if path provided, return local context
  return path.reduce((localCtx, key) => {
    localCtx[key] = localCtx[key] || {}
    return localCtx[key]
  }, ctx)
}

/**
 * Checks if there is a non-empty context at local path (or root if not specified)
 * @param {Express.Request} req Express request
 * @param {string[]} [path] Path to local context (e.g. service context)
 * @returns {boolean} Whether or not a non-empty context object exists
 */
const hasContext = (req, path) => {
  const ctx = context.get(req)
  if (!ctx || !Object.keys(ctx).length) {
    return false
  }
  if (!path) {
    return true
  }
  let localCtx = ctx
  for (const key of path) {
    if (!localCtx[key] || !Object.keys(localCtx[key]).length) {
      return false
    }
    localCtx = localCtx[key]
  }
  return true
}

module.exports = {
  getContext,
  hasContext,
  ERROR_CTX: ['error', 'main'],
  ERROR_QL_CTX: ['error', 'ql'],
}

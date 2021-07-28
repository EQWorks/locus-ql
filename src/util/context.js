// use WeakMap so requests can be garbage collected
const context = new WeakMap()

/**
 * Returns request-scoped context at local path (or root if not specified)
 * A context object will be created if it does not exist
 * @param {Express.Request} req Express request
 * @param {string[]} [path] Path to local context (e.g. service context)
 * @returns {Object} Context object
 */
module.exports.getContext = (req, path) => {
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

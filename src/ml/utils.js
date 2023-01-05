const { createHash } = require('crypto')


const isInternalUser = prefix => ['dev', 'internal'].includes(prefix)

/**
 * Converts all non-array objects to sorted arrays of key/value pairs
 * Can be used to obtain a normalized value of an object
 * @param {Object} object Object
 * @returns {any} Array or non-object value
 */
const sortObject = (object) => {
  // return non-object as is
  if (typeof object !== 'object') {
    return object
  }
  // return null as undefined
  if (object === null) {
    return
  }
  // sort array elements and filter out undefined elements
  // return undefined if empty array
  if (Array.isArray(object)) {
    const objectvalue = object.map(i => sortObject(i)).filter(i => i !== undefined)
    return objectvalue.length ? objectvalue : undefined
  }
  // return object as sorted array of key/value pairs
  // remove undefined keys (e.g. undefined, empty array, null, array with only null entries...)
  const objectValue = Object.entries(object).reduce((value, [k, v]) => {
    const sorted = sortObject(v)
    if (sorted !== undefined) {
      value.push([k, sorted])
    }
    return value
  }, [])
  // return undefined if empty array
  if (!objectValue.length) {
    return
  }
  // sort so results are consistent across calls
  return objectValue.sort(([kA], [kB]) => {
    if (kA < kB) {
      return -1
    }
    if (kA > kB) {
      return 1
    }
    return 0
  })
}

/**
 * Computes a hash for an object based on its JSON value
 * The hash can be used to version the object
 * @param {Object} object Object
 * @returns {string} Hash
 */
const getObjectHash = object => createHash('sha256')
  .update(JSON.stringify(sortObject(object)))
  .digest('base64')

/**
 * Sorts view dependencies by type and removes duplicates
 * @param {Object.<string, {dependencies: [string, number][]}>} views
 * @returns {Object.<string, number[]>} Unique dependencies per type
 */
const sortViewDependencies = (views) => {
  const deps = Object.values(views).map(v => v.dependencies).reduce((uniqueDeps, viewDeps) => {
    if (viewDeps) {
      viewDeps.forEach(([type, id]) => {
        uniqueDeps[type] = uniqueDeps[type] || new Set()
        uniqueDeps[type].add(id)
      })
    }
    return uniqueDeps
  }, {})
  Object.entries(deps).forEach(([key, value]) => {
    deps[key] = [...value]
  })
  return deps
}

module.exports = {
  isInternalUser,
  sortObject,
  getObjectHash,
  sortViewDependencies,
}

const { useAPIErrorOptions } = require('../../util/api-error')


const { apiError } = useAPIErrorOptions({ tags: { service: 'ql' } })

/**
 * Filters out columns which are not present in both input objects
 * @param {Object.<string, Object>} viewColumns View column object
 * @param {Set<string>} queryColumns Query column names
 * @returns {Object.<string, Object>} Filtered column object
 */
const filterViewColumns = (viewColumns, queryColumns) => {
  if (queryColumns.has('*')) {
    return { ...viewColumns }
  }
  return [...queryColumns].reduce((acc, qCol) => {
    if (!(qCol in viewColumns)) {
      throw apiError(`Column '${qCol}' does not exist`, 400)
    }
    acc[qCol] = viewColumns[qCol]
    return acc
  }, {})
}

module.exports = { filterViewColumns }

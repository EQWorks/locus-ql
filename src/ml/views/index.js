/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const { useAPIErrorOptions } = require('../../util/api-error')
const {
  viewTypes,
  viewTypeValues,
  viewCategoryValues,
  listViewCategoriesByViewType,
} = require('./taxonomies')


const { apiError, getSetAPIError } = useAPIErrorOptions({ tags: { service: 'ql' } })

const viewTypesToModules = {
  [viewTypes.EXT]: require('./ext'),
  [viewTypes.GEO]: require('./geo'),
  [viewTypes.LAYER]: require('./layer'),
  [viewTypes.LOGS]: require('./logs'),
  [viewTypes.REPORT_VWI]: require('./reports'),
  [viewTypes.REPORT_WI]: require('./reports'),
  [viewTypes.REPORT_XWI]: require('./reports'),
  [viewTypes.WEATHER]: require('./weather'),
}

// returns views subject to type and category filters
const listViews = async (
  access,
  { inclMeta = false, type, category, filter = {} } = {},
) => {
  if ((type && !(type in viewTypeValues)) || (category && !(category in viewCategoryValues))) {
    throw apiError('Invalid view type or category', 400)
  }

  let typesCategories
  if (category) {
    typesCategories = listViewCategoriesByViewType(category)
  }
  if (type) {
    typesCategories = { [type]: (typesCategories && typesCategories[type]) || undefined }
  }
  // all views
  if (!typesCategories) {
    typesCategories = Object.values(viewTypes).reduce((acc, type) => {
      acc[type] = undefined // no cat filter
      return acc
    }, {})
  }

  const views = await Promise.all(Object.entries(typesCategories).map(([type, categories]) =>
    viewTypesToModules[type].listViews({
      access,
      inclMeta,
      filter: { ...filter, categories },
    })))
  return views.flat()
}


const listViewsMW = async (req, res, next) => {
  try {
    const { access, query: { inclMeta, report, type, category } } = req
    const { subCategory: legacyType1, viewCategory: legacyType2 } = req.query // legacy taxonomy

    // set filters
    const filter = {}
    if (report) {
      // eslint-disable-next-line radix
      const reportID = parseInt(report, 10)
      if (!Number.isNaN(reportID) && reportID > 0) {
        filter.reportID = reportID
      }
    }

    // get views
    const views = await listViews(
      access,
      {
        inclMeta: ['1', 'true'].includes((inclMeta || '').toLowerCase()),
        type: type || legacyType1 || legacyType2,
        category,
        filter,
      },
    )
    res.status(200).json(views)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to retrieve views', 500))
  }
}

/**
 * Retrieves queries and meta necessary to populatea query's views
 * @param {Object} access Access object (typically sourced from req.access)
 * @param {Object<string, Set>} queryColumns Map of views -> columns used by the query
 * @param {string} [engine='pg'] Query engine
 * @returns {Object}
 */
const getQueryViews = async (access, queryColumns, engine = 'pg') => {
  const views = {}
  await Promise.all(Object.entries(queryColumns).map(async ([viewID, columns]) => {
    const [type] = viewID.split('_', 1)
    if (!(type in viewTypeValues)) {
      throw apiError(`Invalid view: ${viewID}`, 400)
    }
    const viewModule = viewTypesToModules[type]
    if (!viewModule) {
      throw apiError(`Failed to retrieve view: ${viewID}`, 500)
    }
    views[viewID] = await viewModule.getQueryView(access, viewID, columns, engine)
  }))
  return views
}

// single view
const getView = (access, viewID) => {
  const [type] = viewID.split('_', 1)
  if (!(type in viewTypeValues)) {
    throw apiError(`Invalid view type: ${type}`, 400)
  }
  const viewModule = viewTypesToModules[type]
  if (!viewModule) {
    throw apiError(`View type not found: ${type}`, 404)
  }
  return viewModule.getView(access, viewID)
}

// load views into req object based on request body.views
const loadQueryViews = async (req, _, next) => {
  try {
    const { access, ql: { tree, engine } } = req
    const queryColumns = tree.viewColumns
    // attach views to req object
    req.ql.views = await getQueryViews(access, queryColumns, engine)
    next()
  } catch (err) {
    next(getSetAPIError(err, 'Failed to load the query views', 500))
  }
}

// returns single view
const getViewMW = async (req, res, next) => {
  try {
    const { access } = req
    const { viewID } = req.params
    const view = await getView(access, viewID)
    res.status(200).json(view)
  } catch (err) {
    next(getSetAPIError(err, 'Failed to rerieve view', 500))
  }
}

module.exports = {
  listViews,
  listViewsMW,
  getQueryViews,
  loadQueryViews,
  getView,
  getViewMW,
}

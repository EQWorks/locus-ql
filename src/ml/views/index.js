/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const { apiError, getSetAPIError } = require('../../util/api-error')
const {
  viewTypes,
  viewTypeValues,
  viewCategoryValues,
  listViewCategoriesByViewType,
} = require('./taxonomies')


const viewTypesToModules = {
  [viewTypes.EXT]: require('./ext'),
  [viewTypes.GEO]: require('./geo'),
  [viewTypes.LAYER]: require('./layer'),
  [viewTypes.LOGS]: require('./logs'),
  [viewTypes.REPORT_VWI]: require('./reports/reportvwi'),
  [viewTypes.REPORT_WI]: require('./reports/reportwi'),
  [viewTypes.REPORT_XWI]: require('./reports/reportxwi'),
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

// returns knex query obj
const getQueryViews = async (access, views, query) => {
  const mlViews = {}
  const mlViewColumns = {}
  const mlViewDependencies = {}
  const mlViewIsInternal = {}
  const mlViewFdwConnections = {}
  await Promise.all(views.map(async (v) => {
    const { type, ...viewParams } = v

    if (!(type in viewTypeValues)) {
      throw apiError(`Invalid view type: ${type}`, 400)
    }

    const viewModule = viewTypesToModules[type]
    if (!viewModule) {
      throw apiError(`View type not found: ${type}`, 400)
    }

    viewParams.query = query
    const view = await viewModule.getQueryView(access, viewParams)
    const { viewID } = view || {}
    if (viewID) {
      mlViews[viewID] = view.mlView
      mlViewColumns[viewID] = view.mlViewColumns
      mlViewDependencies[viewID] = view.mlViewDependencies
      mlViewIsInternal[viewID] = view.mlViewIsInternal
      mlViewFdwConnections[viewID] = view.mlViewFdwConnections
    }
  }))

  return { mlViews, mlViewColumns, mlViewDependencies, mlViewIsInternal, mlViewFdwConnections }
}

// single view
const getView = (access, viewID) => {
  const [, type] = viewID.match(/^([^_]+)_.*$/) || []

  if (!(type in viewTypeValues)) {
    throw apiError(`Invalid view type: ${type}`, 400)
  }

  const viewModule = viewTypesToModules[type]
  if (!viewModule) {
    throw apiError(`View type not found: ${type}`, 400)
  }
  return viewModule.getView(access, viewID)
}

// load views into req object based on request body.views
const loadQueryViews = (onlyUseBodyQuery = false) => async (req, _, next) => {
  try {
    const { access } = req
    let query
    let views
    // if a saved query or execution have been attached to req, use it
    // else use req.body
    const loadedQuery = !onlyUseBodyQuery && (req.mlQuery || req.mlExecution)
    if (loadedQuery) {
      // get views
      ({ query } = loadedQuery)
      const { viewIDs } = loadedQuery
      views = await Promise.all(viewIDs.map(id => getView(access, id).then(v => v.view)))
    } else {
      ({ query, views } = req.body)
    }
    if (!query || !views) {
      throw apiError('Missing field(s): query and/or view')
    }
    const mlViews = await getQueryViews(access, views, query)
    // attach views to req object
    Object.assign(req, mlViews)
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

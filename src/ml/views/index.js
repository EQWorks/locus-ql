/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const { apiError, APIError } = require('../../util/api-error')


const VIEW_LIST = [
  'ext',
  'geo',
  'weather',
  'layer',
  'logs',
  {
    name: 'reports',
    views: ['reportwi', 'reportvwi', 'reportxwi'],
  },
]

const VIEWS = VIEW_LIST.reduce((accViews, view) => {
  if (typeof view === 'object') {
    view.views.forEach((v) => { accViews[v] = require(`./${view.name}/${v}`) })
  } else {
    accViews[view] = require(`./${view}`)
  }
  return accViews
}, {})

// returns all views provided (sub) category
const listViews = async (
  access,
  { viewCategory = 'ext', subCategory, inclMeta = false, filter } = {},
) => {
  const view = (subCategory || viewCategory) in VIEWS
    ? await VIEWS[subCategory || viewCategory].listViews({ access, inclMeta, filter })
    : []

  return VIEW_LIST.reduce((acc, viewCat) => {
    if (typeof viewCat === 'object') {
      acc[viewCat.name] = viewCat.views.map(v => ({
        name: v,
        viewData: v === subCategory ? view : [],
      }))
    } else {
      acc[viewCat] = !subCategory && viewCat === viewCategory ? view : []
    }
    return acc
  }, {})
}


const listViewsMW = async (req, res, next) => {
  try {
    const { access, query: { viewCategory = 'ext', subCategory, inclMeta, report } } = req

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
        viewCategory,
        subCategory,
        inclMeta: ['1', 'true'].includes((inclMeta || '').toLowerCase()),
        filter,
      },
    )
    res.status(200).json(views)
  } catch (err) {
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to retrieve views', 500))
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

    if (!Object.keys(VIEWS).includes(type)) {
      throw apiError(`Invalid view type: ${type}`, 403)
    }

    const viewModule = VIEWS[type]
    if (!viewModule) {
      throw apiError(`View type not found: ${type}`, 403)
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

  if (!Object.keys(VIEWS).includes(type)) {
    throw apiError(`Invalid view type: ${viewID}`, 403)
  }

  const viewModule = VIEWS[type]
  if (!viewModule) {
    throw apiError(`View type not found: ${type}`, 403)
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
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to load the query views', 500))
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
    if (err instanceof APIError) {
      return next(err)
    }
    next(apiError('Failed to rerieve view', 500))
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

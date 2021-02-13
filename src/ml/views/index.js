/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const apiError = require('../../util/api-error')


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
const listViews = async (access, { viewCategory = 'ext', subCategory, inclMeta = false } = {}) => {
  const view = (subCategory || viewCategory) in VIEWS
    ? await VIEWS[subCategory || viewCategory].listViews({ access, inclMeta })
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
    const { access, query: { viewCategory = 'ext', subCategory, inclMeta } } = req
    const views = await listViews(
      access,
      { viewCategory, subCategory, inclMeta: ['1', 'true'].includes(inclMeta) },
    )
    res.status(200).json(views)
  } catch (err) {
    next(err)
  }
}

// returns knex query obj
const getQueryViews = async (access, views, query) => {
  const mlViews = {}
  const mlViewColumns = {}
  const mlViewDependencies = {}
  const mlViewIsInternal = {}
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
    }
  }))

  return { mlViews, mlViewColumns, mlViewDependencies, mlViewIsInternal }
}

// load views into req object based on request body.views
const loadQueryViews = async (req, _, next) => {
  const { views, query } = req.body
  const { access } = req

  try {
    const mlViews = await getQueryViews(access, views, query)
    // attach views to req object
    Object.assign(req, mlViews)
    next()
  } catch (err) {
    // console.error(error)
    return next(err)
  }
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

// returns single view
const getViewMW = async (req, res, next) => {
  try {
    const { access } = req
    const { viewID } = req.params
    const view = await getView(access, viewID)
    res.status(200).json(view)
  } catch (err) {
    return next(err)
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

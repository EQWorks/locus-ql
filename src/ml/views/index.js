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
module.exports.listViews = async (
  { access, query: { viewCategory = 'ext', subCategory } },
  inclMeta = false,
) => {
  const view = await VIEWS[subCategory || viewCategory].listViews({ access, inclMeta })

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

// gets views based on request body.views
module.exports.getViews = async (req, _, next) => {
  const { views, query } = req.body
  const { access } = req

  req.mlViews = {}
  req.mlViewColumns = {}
  try {
    await Promise.all(views.map(async (view) => {
      const { type, id, ...viewParams } = view

      if (!Object.keys(VIEWS).includes(type)) {
        throw apiError(`Invalid view type: ${type}`, 403)
      }

      const viewModule = VIEWS[type]
      if (!viewModule) {
        throw apiError(`View type not found: ${type}`, 403)
      }

      viewParams.query = query || {}
      await viewModule.getView(access, req.mlViews, req.mlViewColumns, viewParams)
    }))
    return next()
  } catch (error) {
    // console.error(error)
    return next(error)
  }
}

// returns single view
module.exports.listView = async (req, res, next) => {
  try {
    const { access } = req
    const { viewID } = req.params
    const [, type] = viewID.match(/^([^_]+)_.*$/) || []

    if (!Object.keys(VIEWS).includes(type)) {
      throw apiError(`Invalid view type: ${viewID}`, 403)
    }

    const viewModule = VIEWS[type]
    if (!viewModule) {
      throw apiError(`View type not found: ${type}`, 403)
    }
    const view = await viewModule.listView(access, viewID)
    res.status(200).json(view)
  } catch (err) {
    return next(err)
  }
}

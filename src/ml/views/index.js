/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const apiError = require('../../util/api-error')


const VIEW_LIST = ['ext', 'geo', 'weather', 'layer', 'logs',
  { reports: ['reportwi', 'reportvwi', 'reportxwi'] },
]
const ALL_VIEWS_LIST = VIEW_LIST.map((v) => {
  if (typeof v === 'object') return Object.values(v)[0]
  return v
}).flat()

// VIEWS structure:
// VIEWS = {
//   report: { listViews, getView },
//   ext: { listViews, getView },
//   ...
// }
const VIEWS = VIEW_LIST.reduce((accViews, viewName) => {
  let view = viewName
  if (typeof viewName === 'object') {
    view = Object.keys(viewName)[0]
    viewName[view].forEach((v) => { accViews[v] = require(`./${view}/${v}`) })
  } else {
    accViews[view] = require(`./${view}`)
  }
  return accViews
}, {})

// return all accessible views
module.exports.listViews = async ({ access, query: { viewCategory = 'ext', subCategory } }) => {
  let view
  if (subCategory) {
    view = await VIEWS[subCategory].listViews(access)
  } else {
    view = await VIEWS[viewCategory].listViews(access)
  }

  return VIEW_LIST.reduce((acc, viewName) => {
    let vn = viewName
    if (typeof viewName === 'object') {
      vn = Object.keys(viewName)[0]
      acc[vn] = viewName[vn].map((v) => {
        if (v === subCategory) {
          return { name: v, viewData: view }
        }
        return { name: v, viewData: [] }
      })
    } else if (viewName === viewCategory) {
      acc[vn] = view
    } else {
      acc[vn] = []
    }
    return acc
  }, {})
}

// get views based on requst body.views
module.exports.getViews = async (req, res, next) => {
  const { views, query } = req.body
  const { access } = req

  req.mlViews = {}
  req.mlViewColumns = {}
  try {
    await Promise.all(views.map(async (view) => {
      const { type, id, ...viewParams } = view

      if (!ALL_VIEWS_LIST.includes(type)) {
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

module.exports.listView = async (req, res, next) => {
  try {
    const { access } = req
    const { viewID } = req.params
    const [, type] = viewID.match(/^([^_]+)_.*$/) || []

    if (!ALL_VIEWS_LIST.includes(type)) {
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

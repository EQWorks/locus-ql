/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const apiError = require('../../util/api-error')


const VIEW_LIST = ['ext', 'report', 'report-vwi', 'geo', 'weather', 'layer', 'logs']

// VIEWS structure:
// VIEWS = {
//   report: { listViews, getView },
//   ext: { listViews, getView },
//   ...
// }
const VIEWS = VIEW_LIST.reduce((accViews, viewName) => {
  accViews[viewName] = require(`./${viewName}`)
  return accViews
}, {})

// return all accessible views
module.exports.listViews = async (access) => {
  const viewPromises = Object.values(VIEWS).map(view => view.listViews(access))
  const views = await Promise.all(viewPromises)

  return VIEW_LIST.reduce((acc, viewName, index) => {
    acc[viewName] = views[index]
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

      if (!VIEW_LIST.includes(type)) {
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
    if (!VIEW_LIST.includes(type)) {
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

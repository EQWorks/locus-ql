/* eslint-disable global-require */
/* eslint-disable import/no-dynamic-require */

const apiError = require('../util/api-error')


module.exports.getViews = async (req, res, next) => {
  const { views } = req.body
  const { access } = req

  req.locusMLViews = {}
  await Promise.all(views.map(async (view) => {
    const { type, id, ...viewParams } = view

    if (!['ext', 'report'].includes(type)) {
      throw apiError(`Invalid view type: ${type}`, 403)
    }

    const viewModule = require(`./views/${type}`)

    if (!viewModule) {
      throw apiError(`View type not found: ${type}`, 403)
    }

    await viewModule.getView(access, req.locusMLViews, viewParams)
  }))
  return next()
}

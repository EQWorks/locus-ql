const report = require('./report')
const ext = require('./ext')
const geo = require('./geo')


module.exports.getAllViews = async (access) => {
  const reportViewsPromise = report.listViews(access)
  const extViewsPromise = ext.listViews(access)
  const geoViewsPromise = geo.listViews()

  const [reportViews, extViews, geoViews] = await Promise.all([
    reportViewsPromise, extViewsPromise, geoViewsPromise,
  ])

  return {
    ext: extViews,
    report: reportViews,
    geo: geoViews,
  }
}

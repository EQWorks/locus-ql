const report = require('./report')
const ext = require('./ext')


module.exports.getAllViews = async (access) => {
  const reportViewsPromise = report.listViews(access)
  const extViewsPromise = ext.listViews(access)

  const [reportViews, extViews] = await Promise.all([reportViewsPromise, extViewsPromise])

  return {
    ext: extViews,
    report: reportViews,
  }
}

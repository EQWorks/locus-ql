/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { listViewsMW, getViewMW, loadQueryViews } = require('../../ml/views')
const {
  queueExecution,
  listExecutions,
  loadExecution,
  respondWithExecution,
} = require('../../ml/executions')
const {
  listQueries,
  postQuery,
  putQuery,
  deleteQuery,
  loadQuery,
  respondWithQuery,
} = require('../../ml/queries')
const { validateQuery } = require('../../ml/engine')
const { accessHasSingleCustomer } = require('../../middleware/validation')


const router = express.Router()

// LEGACY ROUTES
// list out all accessible views with column data -> replaced by GET /views
router.get('/', (req, _, next) => {
  req.query.inclMeta = true
  next()
}, listViewsMW)
// main query endpoint -> replaced by POST /executions
router.post(
  '/executions/',
  loadQuery(false), // run saved query
  loadExecution(false), // duplicate execution (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews,
  validateQuery, queueExecution,
)

// VIEWS
// list out all accessible views without column nor meta data
router.get('/views/', listViewsMW)
// return view object for viewID
router.get('/views/:viewID', getViewMW)


// QUERY EXECUTIONS
router.get('/executions/:id(\\d+)', loadExecution(true), respondWithExecution)
router.get('/executions/', listExecutions)
router.post(
  '/executions/',
  loadQuery(false), // run saved query
  loadExecution(false), // duplicate execution (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews,
  validateQuery, queueExecution,
)

// SAVED QUERIES
router.get('/queries/:id(\\d+)', loadQuery(true), respondWithQuery)
router.put(
  '/queries/:id(\\d+)',
  loadQuery(true),
  loadQueryViews,
  validateQuery,
  putQuery,
)
router.delete(
  '/queries/:id(\\d+)',
  loadQuery(true),
  deleteQuery,
)
router.get('/queries/', listQueries)
router.post(
  '/queries/',
  loadQuery(false), // use saved query as template
  loadExecution(false), // use execution as template (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews,
  validateQuery,
  postQuery,
)

module.exports = router

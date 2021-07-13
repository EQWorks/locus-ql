/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { listViewsMW, getViewMW, loadQueryViews } = require('../../ml/views')
const { getViewCategoryTreeMW } = require('../../ml/views/taxonomies')
const {
  queueExecutionMW,
  listExecutions,
  loadExecution,
  respondWithExecution,
  cancelExecution,
} = require('../../ml/executions')
const {
  listQueries,
  postQuery,
  putQuery,
  deleteQuery,
  loadQuery,
  respondWithQuery,
} = require('../../ml/queries')
const {
  putQuerySchedule,
  deleteQueryScheduleMW,
  listQuerySchedules,
} = require('../../ml/schedules')
const { validateQueryMW } = require('../../ml/engine')
const { accessHasSingleCustomer } = require('../../middleware/validation')


const router = express.Router()

/** -- LEGACY ROUTES -- */

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
  loadQueryViews(),
  validateQueryMW(),
  queueExecutionMW,
)

/* -- TAXONOMIES -- */
/**
 * @api {get} /taxonomies/categories
 * @apiName Get view category tree
 * @apiDescription Returns all categories falling under the supplied root as a tree
 * @apiGroup ml
 * @apiParam (query) {string} [root='root'] View category tree root
*/
router.get('/taxonomies/categories', getViewCategoryTreeMW)

/* -- VIEWS -- */

/**
 * @api {get} /views
 * @apiName List all views
 * @apiDescription Lists out all accessible views without column nor meta data
 * @apiGroup ml
 * @apiParam (query) {string} [category] View category
 * @apiParam (query) {string} [type] View type
 * @apiParam (query) {string} [inclMeta] '1' or 'true' to include columns and other meta data
 * in the response
 * @apiParam (query) {number} [report] ID of the report of interest. Use with report type
 * and/or category
 * @apiParam (query) {string} [viewCategory] DEPRECATED - View category
 * @apiParam (query) {string} [subCategory] DEPRECATED - View subcategory
*/
router.get('/views/', listViewsMW)

/**
 * @api {get} /views/:viewID
 * @apiName Get a specific view
 * @apiDescription Returns view object based on id
 * @apiGroup ml
 * @apiParam (params) {string} viewID ID of the view to return
*/
router.get('/views/:viewID', getViewMW)


/* -- QUERY EXECUTIONS -- */

/**
 * @api {get} /executions/:id
 * @apiName Get a specific execution
 * @apiDescription Returns an execution object based on id
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the execution to return
 * @apiParam (query) {string} [results] '1' or 'true' if results need to be returned
*/
router.get('/executions/:id(\\d+)', loadExecution(true), respondWithExecution)

/**
 * @api {put} /executions/:id
 * @apiName Cancel a specific execution
 * @apiDescription Abort execution
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the execution to return
*/
router.put('/executions/:id(\\d+)', loadExecution(true), cancelExecution)

/**
 * @api {get} /executions
 * @apiName List executions
 * @apiDescription Get a list of executions according to the below filters
 * @apiGroup ml
 * @apiParam (query) {number} [query] ID of the saved query for which executions should be returned
 * @apiParam (query) {number} [limit] Max # of executions to return
 * @apiParam (query) {string} [results] '1' or 'true' if results need to be returned (use along
 * with limit=1)
 * @apiParam (query) {string} [qhash] Query hash
 * @apiParam (query) {string} [chash] Column hash
 * @apiParam (query) {string} [status] Status of the executions
 * @apiParam (query) {number} [start] Start Unix timestamp in seconds
 * @apiParam (query) {number} [end] End Unix timestamp in seconds
*/
router.get('/executions/', listExecutions)

/**
 * @api {post} /executions
 * @apiName Submit a query for execution
 * @apiDescription Customer flags must be sent when none of `query` and `execution` have a
 * value. Returns an object with the `executionID`
 * @apiGroup ml
 * @apiParam (query) {number} [query] ID of the saved query to execute
 * @apiParam (query) {number} [execution] ID of a previous execution to use as template (i.e.
 * rerun). Ignored when `query` has a value
 * @apiParam (Req body) {Object} [query] Query markup. Need only be sent when none of `query`
 * and `execution` have a value
 * @apiParam (Req body) {Array} [views] Views the query depends on. Need only be sent when
 * none of `query` and `execution` have a value
*/
router.post(
  '/executions/',
  loadQuery(false), // run saved query
  loadExecution(false), // duplicate execution (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews(),
  validateQueryMW(),
  queueExecutionMW,
)


/* -- SAVED QUERIES -- */

/**
 * @api {get} /queries/:id
 * @apiName Get a specific query
 * @apiDescription Returns a query object based on id
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the query to return
*/
router.get('/queries/:id(\\d+)', loadQuery(true), respondWithQuery)


/**
 * @api {put} /queries/:id
 * @apiName Update a specific query
 * @apiDescription Updates query based on id. Req body keys not passed will have their value
 * reset. Returns an object with the `queryID`
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the query to update
 * @apiParam (Req body) {string} name Query name
 * @apiParam (Req body) {string} [description] Query description
 * @apiParam (Req body) {Object} query Query markup
 * @apiParam (Req body) {Array} views Views the query depends on
*/
router.put(
  '/queries/:id(\\d+)',
  loadQuery(true),
  loadQueryViews(true),
  validateQueryMW(true),
  putQuery,
)

/**
 * @api {delete} /queries/:id
 * @apiName Delete a specific query
 * @apiDescription Sets a query as 'inactive' based on id. Returns an object with the `queryID`
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the query to delete
*/
router.delete(
  '/queries/:id(\\d+)',
  loadQuery(true),
  deleteQuery,
)

/**
 * @api {get} /queries
 * @apiName List queries
 * @apiDescription Get a list of queries according to the below filters
 * @apiGroup ml
 * @apiParam (query) {number} [execution] ID of the execution for which a query should be returned
 * @apiParam (query) {string} [qhash] Query hash
 * @apiParam (query) {string} [chash] Column hash
*/
router.get('/queries/', listQueries)

/**
 * @api {post} /queries
 * @apiName Create a saved query
 * @apiDescription Customer flags must be sent when none of `query` and `execution` have a
 * value. Returns an object with the `queryID`
 * @apiGroup ml
 * @apiParam (query) {number} [query] ID of the saved query to use as template (i.e duplicate)
 * @apiParam (query) {number} [execution] ID of a previous execution to use as model. Ignored
 * when `query` has a value
 * @apiParam (Req body) {string} name Query name
 * @apiParam (Req body) {string} [description] Query description
 * @apiParam (Req body) {Object} query Query markup. Need only be sent when none of `query` and
 * `execution` have a value
 * @apiParam (Req body) {Array} views Views the query depends on. Need only be sent when none
 * of `query` and `execution` have a value
*/
router.post(
  '/queries/',
  loadQuery(false), // use saved query as template
  loadExecution(false), // use execution as template (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews(),
  validateQueryMW(),
  postQuery,
)


/* -- QUERY SCHEDULES -- */

/**
 * @api {get} /queries/:id/schedules/
 * @apiName Get the list of query schedules
 * @apiDescription Returns an array of schedules attached to a specific query
 * @apiGroup ml
 * @apiParam (params) {number} id ID of the query for which to retrieve schedules
*/
router.get(
  '/queries/:id(\\d+)/schedules/',
  loadQuery(true),
  listQuerySchedules,
)


/**
 * @api {post} /queries/:id/schedules/
 * @apiName Create or update a query schedule
 * @apiDescription Create or update a query schedule as identifed by a query ID and CRON expression
 * @apiGroup ml
 * @apiParam (param) {number} id ID of the saved query the schedule should be attached to
 * @apiParam (Req body) {string} cron CRON expression
 * @apiParam (Req body) {string|number} [startDate] Start date (inclusive) in ISO or epoch format
 * @apiParam (Req body) {string|number} [endDate] End date (inclusive) in ISO or epoch format
 * @apiParam (Req body) {boolean} [isPaused] 'true' if the schedule should be in 'paused' state
*/
router.post(
  '/queries/:id(\\d+)/schedules/',
  loadQuery(true),
  putQuerySchedule,
)

/**
 * @api {delete} /queries/:id/schedules/
 * @apiName Delete a query schedule
 * @apiDescription Delete a query schedule as identifed by a query ID and CRON expression
 * @apiGroup ml
 * @apiParam (param) {number} id ID of the saved query the schedule belongs to
 * @apiParam (Req body) {string} cron CRON expression
*/
router.delete(
  '/queries/:id(\\d+)/schedules/',
  loadQuery(true),
  deleteQueryScheduleMW,
)

module.exports = router

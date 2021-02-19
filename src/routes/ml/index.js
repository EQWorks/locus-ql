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
  loadQueryViews,
  validateQuery,
  queueExecution,
)

/* -- VIEWS -- */

// list out all accessible views without column nor meta data
router.get('/views/', listViewsMW)
// return view object for viewID
router.get('/views/:viewID', getViewMW)


/* -- QUERY EXECUTIONS -- */

/*
Get a specific execution
Route param:
 - id - Execution ID
Query params:
 - results? - 1|true if results need to be returned (use along with limit=1)
Returns an execution object
*/
router.get('/executions/:id(\\d+)', loadExecution(true), respondWithExecution)

/*
Get a list of executions according to the below filters
Query params:
 - query? - ID of the saved query for which executions should be returned
 - results? - 1|true if results need to be returned (use along with limit=1)
 - limit? - Max # of executions to return
 - qhash? - Query hash
 - chash? - Column Hash
 - status? - Status of the executions
 - start? - Start Unix timestamp in seconds
 - end? - End Unix timestamp in seconds
Returns a list of execution results
*/
router.get('/executions/', listExecutions)

/*
Submit a query for execution
Customer flags must be sent when none of `query` and `execution` have a value
Query params:
 - query? - ID of the saved query to execute
 - execution? - ID of a previous execution to use as template (i.e. rerun). Ignored
    when `query` has a value
Body: { query, views }? - Need only be sent when none of `query` and `execution` have a value
Returns an object with the `executionID`
*/
router.post(
  '/executions/',
  loadQuery(false), // run saved query
  loadExecution(false), // duplicate execution (superseded by saved query)
  accessHasSingleCustomer,
  loadQueryViews,
  validateQuery,
  queueExecution,
)


/* -- SAVED QUERIES -- */

/*
Get a specific query
Route param:
 - id - Query ID
Returns a query object
*/
router.get('/queries/:id(\\d+)', loadQuery(true), respondWithQuery)

/*
Update a specific query
Route param:
 - id - Query ID
Body: { name, description?, query, views } - Keys not passed will have their value reset
Returns an object with the `queryID`
*/
router.put(
  '/queries/:id(\\d+)',
  loadQuery(true),
  loadQueryViews,
  validateQuery,
  putQuery,
)

/*
DELETE a specific query
Route param:
 - id - Query ID
Returns an object with the `queryID`
*/
router.delete(
  '/queries/:id(\\d+)',
  loadQuery(true),
  deleteQuery,
)

/*
Get a list of queries according to the below filters
Query params:
 - execution? - ID of the execution for which a query should be returned
 - qhash? - Query hash
 - chash? - Column Hash
Returns a list of query results
*/
router.get('/queries/', listQueries)

/*
Create a saved query
Customer flags must be sent when none of `query` and `execution` have a value
Query params:
 - query? - ID of the saved query to use as template
 - execution? - ID of a previous execution to use as model. Ignored when `query` has a value
Body: { name, description?, query]?, views? } - query and views must be passed when none
   of `query` and `execution` params have a value
Returns an object with the `executionID`
*/
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

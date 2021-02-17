/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { listViewsMW, getViewMW, loadQueryViews } = require('../../ml/views')
const { queueExecution, getExecution, listExecutions } = require('../../ml/executions')
const { getQuery, listQueries, postQuery, putQuery, loadQuery } = require('../../ml/queries')
const { hasCustomerSelector } = require('../../middleware/validation')


const router = express.Router()

// list out all accessible views with column data - legacy (use /views instead)
router.get('/', (req, _, next) => {
  req.query.inclMeta = true
  next()
}, listViewsMW)

// list out all accessible views without column nor meta data
router.get('/views/', listViewsMW)

// return view object for viewID
router.get('/views/:viewID', getViewMW)

// main query endpoint
router.post('/', hasCustomerSelector, loadQueryViews, queueExecution)

// query executions
router.get('/executions/:id(\\d+)', getExecution)
router.get('/executions/', listExecutions)
router.post('/executions/', hasCustomerSelector, loadQuery, loadQueryViews, queueExecution)

// saved queries
router.get('/queries/:id(\\d+)', getQuery)
router.put('/queries/:id(\\d+)', hasCustomerSelector, loadQuery, loadQueryViews, putQuery)
router.get('/queries/', listQueries)
router.post('/queries/', hasCustomerSelector, loadQueryViews, postQuery)

module.exports = router

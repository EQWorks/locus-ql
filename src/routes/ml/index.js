/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { listViewsMW, getViewMW, loadQueryViews } = require('../../ml/views')
const { queueExecution, runQuery, getExecution, listExecutions } = require('../../ml/executions')


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
router.post('/', loadQueryViews, queueExecution, runQuery)

// executions
router.get('/executions/:id(\\d+)', getExecution)
router.get('/executions/', listExecutions)

module.exports = router

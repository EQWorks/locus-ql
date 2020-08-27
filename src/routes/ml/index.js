/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { execute } = require('../../ml/engine')
const { listViews, getViews } = require('../../ml/views')


const router = express.Router()

const mlHandler = async (req, res, next) => {
  const { query } = req.body

  try {
    const result = await execute(req.mlViews, req.mlViewColumns, query)
    console.log('finshed')
    return res.status(200).json(result)
  } catch (error) {
    console.error(error)
    return next(error)
  }
}

// list out all accessible views with column data
router.get('/', (req, res, next) => {
  listViews(req.access).then(data => res.status(200).json(data)).catch(next)
})

// main query endpoint
router.post('/', getViews, mlHandler)

module.exports = router

/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { execute } = require('../../ml/engine')
const { listViews, getViews } = require('../../ml/views')


const router = express.Router()

const mlHandler = async (req, res, next) => {
  const { query } = req.body
  // console.log('query', query);

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
router.get('/', async (req, res, next) => {
  try {
    return res.status(200).json(await listViews(req.access))
  } catch (err) {
    return next(err)
  }
})

// main query endpoint
router.post('/', getViews, mlHandler)

module.exports = router

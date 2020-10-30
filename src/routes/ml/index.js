/* API Document for locus ML */
/* https://github.com/EQWorks/firstorder/wiki/Locus-ML */

const express = require('express')

const { execute } = require('../../ml/engine')
const { listViews, listView, getViews } = require('../../ml/views')
const { getResFromCache, putToCache } = require('../../ml/cache')


const router = express.Router()

const mlHandler = async (req, res, next) => {
  const { query } = req.body

  try {
    const result = await execute(req.mlViews, req.mlViewColumns, query)
    const resultJSON = JSON.stringify(result)
    // store response in cache
    if (req.mlCacheKey) {
      await putToCache(req.mlCacheKey, resultJSON)
    }
    console.log('finished')
    // return res.status(200).json(result)
    return res.status(200).type('application/json').send(resultJSON)
  } catch (error) {
    console.error(error)
    return next(error)
  }
}

// list out all accessible views with column data - legacy (use /views instead)
router.get('/', (req, res, next) => {
  listViews(req, true).then(data => res.status(200).json(data)).catch(next)
})

// list out all accessible views without column nor meta data
router.get('/views/', (req, res, next) => {
  listViews(req).then(data => res.status(200).json(data)).catch(next)
})

// return view object for viewID
router.get('/views/:viewID', listView)

// main query endpoint
router.post('/', getResFromCache, getViews, mlHandler)

module.exports = router

const express = require('express')


const rootRouter = express.Router()
const { jwt } = require('../middleware/auth')

const map = require('./map')
const layer = require('./layer')
const whitelabel = require('./whitelabel')
const requests = require('./requests')
const poilist = require('./poilist')
const poi = require('./poi')
const report = require('./report')
const camp = require('./camp')
const insights = require('./insights')
const api = require('./api')


rootRouter.use(jwt)
rootRouter.use('/map', map)
rootRouter.use('/layer', layer)
rootRouter.use('/whitelabel', whitelabel)
rootRouter.use(['/requests', '/request(-approval)?'], requests)
rootRouter.use('/poilist', poilist)
rootRouter.use('/poi', poi)
rootRouter.use('/report', report)
rootRouter.use('/camp', camp)
rootRouter.use('/insights', insights)
rootRouter.use(api)


module.exports = rootRouter

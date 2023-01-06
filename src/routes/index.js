const express = require('express')

const { loadUserAccess, scopeUserAccess } = require('../middleware/auth')
const { maintenance: { isMaint } } = require('../middleware')

const ql = require('./ml')


const rootRouter = express.Router()
rootRouter.use(isMaint)
rootRouter.use(loadUserAccess, scopeUserAccess)
rootRouter.use(['/ql', '/ml'], ql)

module.exports = rootRouter

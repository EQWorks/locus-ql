const config = require('../../config')
const { Pool } = require('pg')


const pool = new Pool(config.pg)


module.exports = { pool }

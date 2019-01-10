const config = require('../../config')
const { Pool } = require('pg')


const pool = new Pool(config.pg)
const mapPool = new Pool(config.mappingPg)


module.exports = { pool, mapPool }

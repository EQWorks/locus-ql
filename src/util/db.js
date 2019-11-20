const config = require('../../config')
const { Pool } = require('pg')
const Knex = require('knex')


const pool = new Pool(config.pg)
const mapPool = new Pool(config.mappingPg)
const knex = Knex({
  client: 'pg',
  connection: config.pg,
  debug: true,
})
const mapKnex = Knex({
  client: 'pg',
  connection: config.mappingPg,
  debug: true,
})


module.exports = { pool, mapPool, knex, mapKnex }

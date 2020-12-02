const config = require('../../config')
const { Pool } = require('pg')
const Knex = require('knex')


const pool = new Pool({ ...config.pg, max: 1 })
const mapPool = new Pool({ ...config.mappingPg, max: 1 })
const atomPool = new Pool({ ...config.pgAtom, max: 1 })
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

module.exports = { pool, mapPool, atomPool, knex, mapKnex }

const { Client } = require('@eqworks/trino-client-node')


// instantiate client
module.exports = new Client({
  host: process.env.TRINO_HOST,
  port: process.env.TRINO_PORT,
  username: process.env.TRINO_USERNAME,
  password: process.env.TRINO_PASSWORD,
})


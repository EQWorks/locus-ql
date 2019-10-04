const redis = require('redis')
const config = require('../../../config')

const client = redis.createClient(config.redisUrl)
client.on('connect', () => {
  console.log('Redis client connected')
})
client.on('error', (err) => {
  console.log(`Something went wrong ${err}`)
})

module.exports.client = client

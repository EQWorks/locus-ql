const redis = require('redis')
const { redisUrl } = require('../../config')


const client = redis.createClient(redisUrl)
client.on('connect', () => {
  console.log('Redis client connected')
})
client.on('error', (err) => {
  console.log(`Something went wrong ${err}`)
})

module.exports = { client }

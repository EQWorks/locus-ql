const redis = require('redis')


const { REDIS_URL = 'redis://localhost:6379' } = process.env


const client = redis.createClient(REDIS_URL)
client.on('connect', () => {
  console.log('Redis client connected')
})
client.on('error', (err) => {
  console.log(`Something went wrong ${err}`)
})

module.exports = { client }

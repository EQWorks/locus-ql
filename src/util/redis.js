const redis = require('redis')


const _local = 'redis://localhost:6379'
const { REDIS_URL = _local, IS_OFFLINE = false } = process.env

const URL = IS_OFFLINE ? _local : REDIS_URL
const client = redis.createClient(URL)

client.on('connect', () => {
  console.log('Redis client connected')
})
client.on('error', (err) => {
  console.error(err)
})

module.exports = { client }

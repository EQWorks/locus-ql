const redis = require('redis')


const _local = 'redis://localhost:6379'
const { REDIS_URL = _local, IS_OFFLINE = false } = process.env

const URL = IS_OFFLINE ? _local : REDIS_URL
// when invoking a client method with a key that is a Buffer instance,
// client will return a Buffer
const client = redis.createClient(URL, { detect_buffers: true })

client.on('connect', () => {
  console.log('Redis client connected')
})
client.on('error', (err) => {
  console.error(err)
})

module.exports = { client }

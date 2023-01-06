const axios = require('axios')


const maintURL =
  'https://ojuoz0bs90.execute-api.us-east-1.amazonaws.com/dev/maintenance?service=locus'

module.exports.isMaint = async (_, res, next) => {
  try {
    const { data } = await axios.create().get(maintURL)
    if (data && data.start) {
      const { start, end = Infinity, message = 'Scheduled system maintenance' } = data
      const curUnixTS = Date.now() / 1000
      if (start < curUnixTS && curUnixTS < end) {
        // in maintenance
        res.status(503).send(message)
        return
      }
      // maintenance time not in range
      return next()
    }
  } catch (error) {
    if (error.response && error.response.status === 404) {
      // no maintenance found
      return next()
    }
    return next(error)
  }
}

const { lambda } = require('./aws')


// reference from smaug service
// CONFIG_TYPES = {
//   'minute': (int, -1),
//   'hour': (int, -1),
//   'day': (int, -1),
//   'month': (int, -1),
//   'white-label': (str, None),
//   'customer': (str, None),
//   'user': (str, None),
//   'prefix': (str, None),
// }
module.exports.increment = ({ config, n = 1 }) => lambda.invoke({
  FunctionName: 'smaug-dev-increment', // TODO: wire-in different stages
  InvocationType: 'RequestResponse',
  Payload: JSON.stringify({ config, n }),
}).promise()

module.exports.get = ({ config }) => lambda.invoke({
  FunctionName: 'smaug-get-increment', // TODO: wire-in different stages
  InvocationType: 'RequestResponse',
  Payload: JSON.stringify({ config }),
}).promise()

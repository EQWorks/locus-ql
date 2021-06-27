const { lambda } = require('./aws')


// reference from smaug service
// NOTE: always double check against https://github.com/EQWorks/smaug for potential updates
// CONFIG_TYPES = {
//     # 'pre': (bool, False),  # TODO: implement pre/post-pay graceful limit
//     'minute': (int, -1),
//     'hour': (int, -1),
//     'day': (int, -1),
//     'month': (int, -1),
//     'whitelabel': (str, None),
//     'customer': (str, None),
//     'user': (str, None),
//     'prefix': (str, None),
// }
module.exports.increment = ({ config, n = 1 }) => lambda.invoke({
  FunctionName: 'smaug-dev-increment', // TODO: wire-in different stages
  InvocationType: 'RequestResponse',
  Payload: JSON.stringify({ config, n }),
}).promise()

module.exports.get = ({ config }) => lambda.invoke({
  FunctionName: 'smaug-dev-get', // TODO: wire-in different stages
  InvocationType: 'RequestResponse',
  Payload: JSON.stringify({ config }),
}).promise()

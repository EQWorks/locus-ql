// wraps around Error object with an additional status field
// status will get used by API's catch-all error handler
module.exports = (errorMessage, statusCode = 400) => {
  const error = new Error(errorMessage)
  error.status = statusCode
  return error
}

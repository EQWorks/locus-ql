// API errors are safe to return to the client
class APIError extends Error {
  constructor(errorMessage, statusCode = 400) {
    super(errorMessage)
    this.statusCode = statusCode
  }
}

// wraps around Error object with an additional status field
// status will get used by API's catch-all error handler
module.exports = {
  APIError,
  apiError: (errorMessage, statusCode = 400) => new APIError(errorMessage, statusCode),
}

// API errors are safe to return to the client
class APIError extends Error {
  constructor(errorMessage, statusCode = 400, originalError) {
    super(errorMessage)
    this.status = statusCode
    if (originalError) {
      this.originalError = originalError
    }
  }
}

const getSetAPIError = (originalError, errorMessage, statusCode) => {
  if (originalError instanceof APIError) {
    return originalError
  }
  return new APIError(errorMessage, statusCode, originalError)
}

const apiError = (errorMessage, statusCodeOrOptions = 400) => {
  // case options
  if (typeof statusCodeOrOptions === 'object') {
    const { statusCode, originalError } = statusCodeOrOptions
    return new APIError(errorMessage, statusCode, originalError)
  }
  // case status code
  return new APIError(errorMessage, statusCodeOrOptions)
}

// wraps around Error object with an additional status field
// status will get used by API's catch-all error handler
module.exports = {
  APIError,
  apiError,
  getSetAPIError,
}

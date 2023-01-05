// API errors are safe to return to the client
class APIError extends Error {
  constructor(errorMessage, { statusCode, originalError, level, tags } = {}) {
    super(errorMessage)
    this.status = statusCode || 400
    if (originalError) {
      this.originalError = originalError
    }
    this.level = level || (statusCode >= 500 ? 'error' : 'warning')
    // { tagKey2: tagValue1, tagKey2: tagValue2, ... }
    if (tags) {
      this.tags = tags
    }
  }
}

const getErrorOptions = (statusCodeOrOptions, defaultOptions) => {
  const options = defaultOptions ? { ...defaultOptions } : {}
  if (!statusCodeOrOptions) {
    return options
  }

  // case options
  if (typeof statusCodeOrOptions === 'object') {
    return Object.assign(options, statusCodeOrOptions)
  }

  // case status code
  options.statusCode = statusCodeOrOptions
  return options
}

const _getSetAPIError = (originalError, errorMessage, statusCodeOrOptions, defaultOptions) => {
  if (originalError instanceof APIError) {
    return originalError
  }
  const options = getErrorOptions(statusCodeOrOptions, defaultOptions)
  options.originalError = originalError
  return new APIError(errorMessage, options)
}

const _apiError = (errorMessage, statusCodeOrOptions, defaultOptions) => {
  const options = getErrorOptions(statusCodeOrOptions, defaultOptions)
  return new APIError(errorMessage, options)
}

const getSetAPIError = (originalError, errorMessage, statusCodeOrOptions) =>
  _getSetAPIError(originalError, errorMessage, statusCodeOrOptions)

const apiError = (errorMessage, statusCodeOrOptions) =>
  _apiError(errorMessage, statusCodeOrOptions)

const useAPIErrorOptions = defaultOptions => ({
  getSetAPIError: (originalError, errorMessage, statusCodeOrOptions) =>
    _getSetAPIError(originalError, errorMessage, statusCodeOrOptions, defaultOptions),
  apiError: (errorMessage, statusCodeOrOptions) =>
    _apiError(errorMessage, statusCodeOrOptions, defaultOptions),
})


module.exports = {
  APIError,
  apiError,
  getSetAPIError,
  useAPIErrorOptions,
}

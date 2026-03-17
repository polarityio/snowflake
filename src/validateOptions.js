'use strict';

/**
 * Validates that required string options are non-empty.
 * @param {Object} errorMessages - Map of option key → error message string
 * @param {Object} options - Polarity options object
 * @returns {Array} Array of { key, message } error objects
 */
function validateStringOptions(errorMessages, options) {
  return Object.entries(errorMessages).reduce((errors, [key, message]) => {
    const value = options[key] && options[key].value;
    if (!value || (typeof value === 'string' && !value.trim())) {
      errors.push({ key, message });
    }
    return errors;
  }, []);
}

/**
 * Validates that a URL string is well-formed.
 * @param {string} urlValue
 * @param {Array} existingErrors
 * @returns {Array}
 */
function validateUrlOption(urlValue, existingErrors) {
  if (!urlValue) return existingErrors;
  try {
    new URL(urlValue.startsWith('http') ? urlValue : `https://${urlValue}`);
    return existingErrors;
  } catch (_) {
    return [
      ...existingErrors,
      { key: 'baseUrl', message: 'The provided URL is not valid. Ensure it does not include a trailing slash.' }
    ];
  }
}

module.exports = { validateStringOptions, validateUrlOption };

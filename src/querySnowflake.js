'use strict';

const { v4: uuidv4 } = require('uuid');
const request = require('postman-request');

/**
 * Wraps postman-request in a Promise and handles Snowflake SQL API response codes.
 *
 * Throws a structured error object for non-2xx responses except 202 (async in-progress).
 * Returns { status, body } for 200 and 202.
 */
function makeRequest({ method, url, token, authType, body, logger }) {
  return new Promise((resolve, reject) => {
    const options = {
      method,
      url,
      json: true,
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        'User-Agent': 'polarity-snowflake-integration/3.1.0',
        Authorization: `Bearer ${token}`,
        'X-Snowflake-Authorization-Token-Type': authType
      }
    };

    if (body) options.body = body;

    if (logger) {
      logger.trace(
        { method, url, authType, hasBody: !!body, bodyStatement: body && body.statement },
        'Snowflake HTTP request'
      );
    }

    request(options, (err, response, responseBody) => {
      if (err) {
        if (logger) logger.error({ url, err: err.message }, 'Snowflake HTTP network error');
        return reject({ message: err.message, isNetworkError: true });
      }

      const status = response.statusCode;

      if (logger) {
        logger.trace(
          {
            url,
            status,
            statementHandle: responseBody && responseBody.statementHandle,
            numRows: responseBody && responseBody.resultSetMetaData && responseBody.resultSetMetaData.numRows,
            dataLength: responseBody && responseBody.data && responseBody.data.length,
            message: responseBody && responseBody.message,
            code: responseBody && responseBody.code
          },
          'Snowflake HTTP response'
        );
      }

      if (status === 200 || status === 202) {
        return resolve({ status, body: responseBody });
      }

      // Map Snowflake error codes to user-friendly messages
      const snowflakeMessage = responseBody?.message || responseBody?.data?.message || '';
      const sqlState = responseBody?.sqlState || '';

      let userMessage;
      switch (status) {
        case 400:
          userMessage = `Bad request: ${snowflakeMessage || 'Malformed query or request body.'}`;
          break;
        case 401:
          userMessage = 'Authentication failed — check credentials in the integration settings.';
          break;
        case 403:
          userMessage = 'Forbidden — verify the SQL API is enabled for your Snowflake account.';
          break;
        case 404:
          userMessage = 'Endpoint not found — verify the Account Identifier in settings.';
          break;
        case 408:
          userMessage = 'Query timed out — consider increasing the Query Timeout setting or optimising your SQL.';
          break;
        case 415:
          userMessage = 'Unsupported Content-Type (integration bug — contact support).';
          break;
        case 422:
          userMessage = `SQL error${sqlState ? ` [${sqlState}]` : ''}: ${snowflakeMessage || 'Unknown SQL compilation error.'}`;
          break;
        case 429:
          userMessage = 'Rate limit exceeded — Snowflake concurrency limit reached. Please retry shortly.';
          break;
        default:
          userMessage = `Snowflake returned status ${status}: ${snowflakeMessage || 'Unknown error.'}`;
      }

      const error = new Error(userMessage);
      error.status = status;
      error.userMessage = userMessage;
      error.rawBody = responseBody;
      reject(error);
    });
  });
}

/**
 * Submits a SQL statement to the Snowflake SQL API.
 * Always uses async=true so we get a handle immediately.
 */
async function submitStatement({ baseUrl, token, authType, body, logger }) {
  const requestId = uuidv4();
  const url = `${baseUrl}/api/v2/statements?requestId=${requestId}&async=true`;
  return makeRequest({ method: 'POST', url, token, authType, body, logger });
}

/**
 * Polls for the result of an async statement.
 * Returns { status: 200, body } when complete or { status: 202 } when still running.
 */
async function pollStatement({ baseUrl, token, authType, statementHandle, logger }) {
  const url = `${baseUrl}/api/v2/statements/${statementHandle}`;
  return makeRequest({ method: 'GET', url, token, authType, logger });
}

/**
 * Cancels an in-flight statement. Non-fatal — errors are swallowed.
 */
async function cancelStatement({ baseUrl, token, authType, statementHandle, logger }) {
  const url = `${baseUrl}/api/v2/statements/${statementHandle}/cancel`;
  try {
    await makeRequest({ method: 'POST', url, token, authType, logger });
  } catch (_) {
    // Cancel errors are non-fatal
  }
}

module.exports = { submitStatement, pollStatement, cancelStatement };

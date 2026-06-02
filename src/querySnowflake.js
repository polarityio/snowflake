'use strict';

const { v4: uuidv4 } = require('uuid');
const request = require('postman-request');

const VERSION = '1.0.0';
const USER_AGENT = `polarity-snowflake-integration/${VERSION}`;

// 5xx retry configuration (gateway/transient failures only).
const RETRYABLE_STATUS_CODES = new Set([500, 502, 503, 504]);
const RETRY_BACKOFF_MS = [1000, 2000, 4000];

// Snowflake error codes that indicate the warehouse is suspended/resuming.
// Reference: https://docs.snowflake.com/en/sql-reference/error-code-reference
const WAREHOUSE_SUSPENDED_CODES = new Set(['000605', '000627']);
const WAREHOUSE_SUSPENDED_REGEX = /(warehouse[\s_]+is[\s_]+(suspended|not[\s_]+resumed|currently[\s_]+(starting|resuming))|please[\s_]+resume[\s_]+the[\s_]+warehouse)/i;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isWarehouseSuspended(responseBody) {
  if (!responseBody) return false;
  const code = (responseBody.code || responseBody?.data?.errorCode || '').toString();
  if (WAREHOUSE_SUSPENDED_CODES.has(code)) return true;
  const msg = responseBody.message || responseBody?.data?.message || '';
  return WAREHOUSE_SUSPENDED_REGEX.test(msg);
}

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
        'User-Agent': USER_AGENT,
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
        const networkErr = new Error(err.message || 'Network error contacting Snowflake');
        networkErr.isNetworkError = true;
        networkErr.userMessage = 'Could not reach Snowflake — verify the Account Identifier and network connectivity.';
        return reject(networkErr);
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
            partitionInfo: responseBody && responseBody.resultSetMetaData && responseBody.resultSetMetaData.partitionInfo,
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
      let isAuthError = false;
      let isWarehouseError = false;

      if (isWarehouseSuspended(responseBody)) {
        isWarehouseError = true;
        userMessage = 'Warehouse is suspended or resuming — Snowflake will auto-resume. Retry the lookup in a moment.';
      } else {
        switch (status) {
          case 400:
            userMessage = `Bad request: ${snowflakeMessage || 'Malformed query or request body.'}`;
            break;
          case 401:
            isAuthError = true;
            userMessage =
              authType === 'OAUTH'
                ? 'OAuth token expired or invalid — refresh the OAuth Token in the integration options.'
                : 'Authentication failed — verify the Username, Account Identifier, and Private Key.';
            break;
          case 403:
            userMessage = 'Forbidden — verify the SQL API is enabled and the role has access to the queried objects.';
            break;
          case 404:
            userMessage = 'Endpoint not found — verify the Account Identifier in settings.';
            break;
          case 408:
            userMessage = 'Query timed out — increase the Query Timeout setting or simplify your SQL.';
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
      }

      const error = new Error(userMessage);
      error.status = status;
      error.userMessage = userMessage;
      error.rawBody = responseBody;
      error.isAuthError = isAuthError;
      error.isWarehouseError = isWarehouseError;
      error.isRetryableServerError = RETRYABLE_STATUS_CODES.has(status);
      reject(error);
    });
  });
}

/**
 * Internal: makeRequest with built-in retry on 5xx (transient gateway errors).
 *
 * The same `requestId` is reused across retries (per Snowflake guidance) so the
 * server can deduplicate if the prior attempt actually committed.
 */
async function makeRequestWithRetry({ method, url, token, authType, body, logger }) {
  let lastError;
  for (let attempt = 0; attempt <= RETRY_BACKOFF_MS.length; attempt++) {
    try {
      return await makeRequest({ method, url, token, authType, body, logger });
    } catch (err) {
      lastError = err;
      // Only retry on transient 5xx — never on 4xx/auth/network errors.
      if (!err.isRetryableServerError || attempt >= RETRY_BACKOFF_MS.length) {
        throw err;
      }
      const delay = RETRY_BACKOFF_MS[attempt];
      if (logger) {
        logger.warn(
          { status: err.status, url, attempt: attempt + 1, retryInMs: delay },
          'Snowflake transient error — retrying'
        );
      }
      await sleep(delay);
    }
  }
  throw lastError;
}

/**
 * Submits a SQL statement to the Snowflake SQL API.
 * Always uses async=true so we get a handle immediately.
 *
 * The same requestId is reused across 5xx retries to allow Snowflake to
 * deduplicate at the server side.
 */
async function submitStatement({ baseUrl, token, authType, body, logger, requestId }) {
  const id = requestId || uuidv4();
  const url = `${baseUrl}/api/v2/statements?requestId=${id}&async=true`;
  const result = await makeRequestWithRetry({ method: 'POST', url, token, authType, body, logger });
  result.requestId = id;
  return result;
}

/**
 * Polls for the result of an async statement.
 * Returns { status: 200, body } when complete or { status: 202 } when still running.
 *
 * `partition` (0-based) selects which partition of the result set to fetch.
 * Defaults to 0. When omitted Snowflake returns partition 0 plus partitionInfo.
 */
async function pollStatement({ baseUrl, token, authType, statementHandle, partition, logger }) {
  const url =
    `${baseUrl}/api/v2/statements/${statementHandle}` +
    (typeof partition === 'number' ? `?partition=${partition}` : '');
  return makeRequestWithRetry({ method: 'GET', url, token, authType, logger });
}

/**
 * Cancels an in-flight statement. Non-fatal — errors are logged but never thrown.
 */
async function cancelStatement({ baseUrl, token, authType, statementHandle, logger }) {
  const url = `${baseUrl}/api/v2/statements/${statementHandle}/cancel`;
  try {
    await makeRequest({ method: 'POST', url, token, authType, logger });
    if (logger) logger.info({ statementHandle }, 'Snowflake statement cancelled');
  } catch (err) {
    if (logger) {
      logger.warn(
        { statementHandle, status: err.status, message: err.message },
        'Snowflake cancel call failed (non-fatal)'
      );
    }
  }
}

module.exports = {
  submitStatement,
  pollStatement,
  cancelStatement,
  isWarehouseSuspended,
  USER_AGENT,
  VERSION
};

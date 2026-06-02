'use strict';

const postman = require('postman-request');

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

/**
 * Connectivity probe — verifies the configured base URL resolves and the
 * provided credentials authenticate against the Snowflake SQL API.
 *
 * Probes the statement-status endpoint with a sentinel UUID. Snowflake will
 * return:
 *   - 404 / 422 → API is reachable, auth worked, sentinel handle just doesn't exist (✅ healthy)
 *   - 401       → auth failed (bad OAuth token, expired token, wrong key-pair user)
 *   - 5xx       → likely wrong base URL or transient API outage
 *   - DNS / connection error → wrong account identifier
 *
 * Returns an array of { key, message } error objects (empty if healthy).
 * Best-effort — does not throw; logs and returns an empty array on
 * unexpected internal failures so that we don't block configuration save
 * on probe-side bugs.
 *
 * @param {Object} args
 * @param {string} args.baseUrl   - https://<account>.snowflakecomputing.com
 * @param {string} args.token     - Bearer token (OAuth or freshly-generated JWT)
 * @param {string} args.authType  - 'OAUTH' | 'KEYPAIR_JWT'
 * @param {Object} args.logger    - Polarity logger
 * @returns {Promise<Array>}
 */
function validateConnectivity({ baseUrl, token, authType, logger }) {
  return new Promise((resolve) => {
    const sentinel = '00000000-0000-0000-0000-000000000000';
    const headers = {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
      'User-Agent': 'polarity-snowflake-integration/1.0.0'
    };
    if (authType === 'KEYPAIR_JWT') {
      headers['X-Snowflake-Authorization-Token-Type'] = 'KEYPAIR_JWT';
    }

    postman(
      {
        method: 'GET',
        url: `${baseUrl}/api/v2/statements/${sentinel}`,
        headers,
        json: true,
        timeout: 8000
      },
      (err, response, body) => {
        if (err) {
          // DNS / connection refused / TLS / timeout → almost always means the
          // account identifier is wrong or the host is unreachable.
          if (logger) {
            logger.warn(
              { err: { code: err.code, message: err.message }, baseUrl },
              'Connectivity probe — network error'
            );
          }
          const code = err.code || '';
          let message = `Could not reach Snowflake at ${baseUrl}: ${err.message || code}`;
          if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
            message = `Could not resolve ${baseUrl}. Check the Account Identifier — it should look like xy12345.us-east-1 (no protocol, no trailing slash).`;
          } else if (code === 'ETIMEDOUT' || code === 'ESOCKETTIMEDOUT') {
            message = `Connection to ${baseUrl} timed out. Check your network or proxy settings.`;
          }
          return resolve([{ key: 'accountIdentifier', message }]);
        }

        const status = response && response.statusCode;
        if (logger) {
          logger.trace({ status, baseUrl }, 'Connectivity probe — response status');
        }

        // 404 / 422 → API is reachable and auth worked; the sentinel handle just doesn't exist.
        if (status === 404 || status === 422) {
          return resolve([]);
        }

        // 401 → auth failed.
        if (status === 401) {
          const message =
            authType === 'OAUTH'
              ? 'OAuth token authentication failed. The token may be expired, revoked, or scoped incorrectly. Generate a fresh OAuth token in Snowflake and update the integration.'
              : 'Key-Pair JWT authentication failed. Verify the Username matches the Snowflake user, the Public Key has been registered with ALTER USER ... SET RSA_PUBLIC_KEY = ..., and the Private Key passphrase is correct.';
          return resolve([
            { key: authType === 'OAUTH' ? 'oauthToken' : 'privateKey', message }
          ]);
        }

        // 403 → auth worked but the bearer was rejected for the resource.
        // Treat as auth-adjacent — almost always the same root cause as 401.
        if (status === 403) {
          return resolve([
            {
              key: authType === 'OAUTH' ? 'oauthToken' : 'privateKey',
              message:
                'Snowflake returned 403 Forbidden during connectivity probe. The bearer token authenticated but was rejected — verify the user has USAGE on the configured warehouse/role.'
            }
          ]);
        }

        // 5xx → almost always wrong base URL / not actually a Snowflake host.
        if (status >= 500) {
          return resolve([
            {
              key: 'accountIdentifier',
              message: `Snowflake responded with HTTP ${status}. The Account Identifier may be wrong, or there is a Snowflake-side outage.`
            }
          ]);
        }

        // Anything else → don't block save. Log and pass.
        if (logger) {
          logger.warn(
            { status, body },
            'Connectivity probe — unexpected status (allowing config save)'
          );
        }
        resolve([]);
      }
    );
  });
}

module.exports = { validateStringOptions, validateUrlOption, validateConnectivity };

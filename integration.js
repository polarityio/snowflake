'use strict';

const { generateJwt, isJwtExpired } = require('./src/generateJwt');
const { submitStatement, pollStatement, cancelStatement } = require('./src/querySnowflake');
const { mapResultRows, buildSummaryTags, parseAttributeList, parseErrorToReadableJSON } = require('./src/dataTransformations');
const { validateStringOptions, validateUrlOption } = require('./src/validateOptions');

let Logger;
// JWT cache: { token, expiresAt }
let jwtCache = null;

const MAX_POLL_ATTEMPTS = 5;
const POLL_INTERVALS_MS = [500, 1000, 2000, 3000, 4000];

const startup = (logger) => {
  Logger = logger;
};

/**
 * Returns a valid bearer token for the configured auth type.
 * For OAuth: returns options.oauthToken directly.
 * For Key-Pair JWT: generates a fresh JWT or returns the cached one if still valid.
 */
async function getToken(options) {
  const authType = options.authType.value;
  if (authType === 'oauth') {
    return options.oauthToken;
  }

  // Key-pair JWT — use cache unless expired
  if (jwtCache && !isJwtExpired(jwtCache.expiresAt)) {
    Logger.trace('Using cached JWT');
    return jwtCache.token;
  }

  Logger.debug('Generating new JWT for key-pair auth');
  const { token, expiresAt } = generateJwt({
    accountIdentifier: options.accountIdentifier,
    username: options.username,
    privateKey: options.privateKey,
    privateKeyPassphrase: options.privateKeyPassphrase || ''
  });
  jwtCache = { token, expiresAt };
  return token;
}

/**
 * Polls a pending statement handle until complete or max attempts reached.
 * Returns { complete: true, rows, metadata } or { complete: false, statementHandle, elapsed }.
 */
async function pollToCompletion(statementHandle, baseUrl, token, authType, startTime) {
  for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
    await sleep(POLL_INTERVALS_MS[attempt]);
    const result = await pollStatement({ baseUrl, token, authType, statementHandle });
    if (result.status === 200) {
      return { complete: true, resultSet: result.body, elapsedMs: Date.now() - startTime };
    }
    Logger.debug({ attempt, statementHandle }, 'Query still running — retrying poll');
  }
  // Exceeded poll budget — return handle for the frontend "Check Status" button
  return { complete: false, statementHandle, elapsedMs: Date.now() - startTime };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Builds the base URL from the account identifier.
 */
function buildBaseUrl(accountIdentifier) {
  return `https://${accountIdentifier}.snowflakecomputing.com`;
}

/**
 * Counts ? placeholders in a query and builds a bindings map
 * where every position receives the same entity value.
 */
function buildBindings(query, entityValue, bindingType) {
  const matches = query.match(/\?/g) || [];
  const bindings = {};
  matches.forEach((_, i) => {
    bindings[String(i + 1)] = { type: bindingType, value: entityValue };
  });
  return bindings;
}

const doLookup = async (entities, options, cb) => {
  Logger.debug({ entities: entities.map((e) => e.value) }, 'doLookup');

  let token;
  try {
    token = await getToken(options);
  } catch (err) {
    const readable = parseErrorToReadableJSON(err);
    Logger.error({ err: readable }, 'Failed to obtain auth token');
    return cb({ detail: 'Authentication failed — check credentials in integration settings.', err: readable });
  }

  const baseUrl = buildBaseUrl(options.accountIdentifier);
  const authType = options.authType.value === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
  const bindingType = options.bindingType.value;
  const query = options.query;
  const queryTimeout = Number(options.queryTimeout) || 30;
  const resultLimit = Number(options.resultLimit) || 100;

  const summaryAttrList = parseAttributeList(options.summaryAttributes);
  const detailAttrList = parseAttributeList(options.detailAttributes);
  const itemTitleAttr = (options.itemTitleAttribute || '').trim().toUpperCase();
  const maxSummaryItems = Number(options.maxSummaryItems) || 3;

  const lookupResults = await Promise.all(
    entities.map(async (entity) => {
      const startTime = Date.now();
      let statementHandle;

      try {
        const bindings = buildBindings(query, entity.value, bindingType);
        const body = {
          statement: query,
          timeout: queryTimeout,
          parameters: {
            rows_per_resultset: resultLimit,
            query_tag: 'polarity-integration',
            use_cached_result: 'true'
          },
          bindings
        };
        if (options.warehouse) body.warehouse = options.warehouse;
        if (options.database) body.database = options.database;
        if (options.schema) body.schema = options.schema;
        if (options.role) body.role = options.role;

        const submitResult = await submitStatement({ baseUrl, token, authType, body });

        if (submitResult.status === 200) {
          // Synchronous result
          return buildLookupResult(entity, submitResult.body, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, Date.now() - startTime);
        }

        if (submitResult.status === 202) {
          // Async — poll
          statementHandle = submitResult.body.statementHandle;
          const pollResult = await pollToCompletion(statementHandle, baseUrl, token, authType, startTime);

          if (pollResult.complete) {
            return buildLookupResult(entity, pollResult.resultSet, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, pollResult.elapsedMs);
          }

          // Not yet complete — return pending state for onMessage
          return {
            entity,
            data: {
              summary: ['⏳ Query Running'],
              details: {
                complete: false,
                statementHandle,
                elapsedMs: pollResult.elapsedMs,
                executionStats: { elapsedSeconds: (pollResult.elapsedMs / 1000).toFixed(1) }
              }
            }
          };
        }

        // Unexpected status from submit
        Logger.error({ status: submitResult.status, body: submitResult.body }, 'Unexpected submit status');
        return buildErrorResult(entity, `Unexpected response status ${submitResult.status} from Snowflake.`);
      } catch (err) {
        Logger.error({ entity: entity.value, err: parseErrorToReadableJSON(err) }, 'Entity lookup error');
        return buildErrorResult(entity, err.userMessage || err.message || 'Lookup failed');
      }
    })
  );

  Logger.trace({ lookupResults }, 'Lookup Results');
  cb(null, lookupResults);
};

function buildLookupResult(entity, resultSet, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, elapsedMs) {
  const rows = mapResultRows(resultSet, detailAttrList, itemTitleAttr);

  if (rows.length === 0) {
    return { entity, data: null };
  }

  const summaryTags = buildSummaryTags(rows, summaryAttrList, maxSummaryItems);
  const { resultSetMetaData, statementHandle, message } = resultSet;
  const partitionCount = (resultSetMetaData?.partitionInfo || []).length;
  const isTruncated = partitionCount > 1;

  return {
    entity,
    data: {
      summary: summaryTags,
      details: {
        complete: true,
        results: rows,
        statementHandle: statementHandle || '',
        executionStats: {
          elapsedSeconds: (elapsedMs / 1000).toFixed(2),
          numRows: resultSetMetaData?.numRows ?? rows.length,
          partitionCount,
          isTruncated
        },
        queryContext: {
          message: message || '',
          createdOn: resultSet.createdOn || null
        }
      }
    }
  };
}

function buildErrorResult(entity, message) {
  return {
    entity,
    data: {
      summary: ['⚠ Query Error'],
      details: {
        complete: true,
        isError: true,
        errorMessage: message,
        results: []
      }
    }
  };
}

const onMessage = async (payload, options, cb) => {
  if (payload.action !== 'CHECK_QUERY_STATUS') {
    return cb({ detail: `Unknown action: ${payload.action}` });
  }

  let token;
  try {
    token = await getToken(options);
  } catch (err) {
    return cb({ detail: 'Authentication failed — check credentials.' });
  }

  const baseUrl = buildBaseUrl(options.accountIdentifier);
  const authType = options.authType.value === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
  const { statementHandle } = payload;

  try {
    const result = await pollStatement({ baseUrl, token, authType, statementHandle });

    if (result.status === 200) {
      const summaryAttrList = parseAttributeList(options.summaryAttributes);
      const detailAttrList = parseAttributeList(options.detailAttributes);
      const itemTitleAttr = (options.itemTitleAttribute || '').trim().toUpperCase();
      const maxSummaryItems = Number(options.maxSummaryItems) || 3;

      const rows = mapResultRows(result.body, detailAttrList, itemTitleAttr);
      const summaryTags = buildSummaryTags(rows, summaryAttrList, maxSummaryItems);
      const { resultSetMetaData, statementHandle: handle, message } = result.body;
      const partitionCount = (resultSetMetaData?.partitionInfo || []).length;

      return cb(null, {
        summary: summaryTags,
        details: {
          complete: true,
          results: rows,
          statementHandle: handle || statementHandle,
          executionStats: {
            numRows: resultSetMetaData?.numRows ?? rows.length,
            partitionCount,
            isTruncated: partitionCount > 1
          },
          queryContext: { message: message || '' }
        }
      });
    }

    // Still running
    cb(null, {
      summary: ['⏳ Query Running'],
      details: {
        complete: false,
        statementHandle,
        executionStats: { elapsedSeconds: '…' }
      }
    });
  } catch (err) {
    Logger.error({ err: parseErrorToReadableJSON(err) }, 'onMessage poll error');
    cb({ detail: err.userMessage || err.message || 'Failed to check query status.' });
  }
};

const validateOptions = async (options, callback) => {
  const errors = [];

  // Account identifier required
  if (!options.accountIdentifier || !options.accountIdentifier.value) {
    errors.push({ key: 'accountIdentifier', message: 'You must provide a Snowflake Account Identifier.' });
  }

  // Auth-type-specific validation
  const authType = options.authType?.value?.value || options.authType?.value;
  if (authType === 'oauth') {
    if (!options.oauthToken || !options.oauthToken.value) {
      errors.push({ key: 'oauthToken', message: 'An OAuth Token is required when Authentication Type is set to "OAuth Token".' });
    }
  } else if (authType === 'keypair') {
    if (!options.username || !options.username.value) {
      errors.push({ key: 'username', message: 'A Username is required for Key-Pair JWT authentication.' });
    }
    if (!options.privateKey || !options.privateKey.value) {
      errors.push({ key: 'privateKey', message: 'A Private Key (PEM) is required for Key-Pair JWT authentication.' });
    }
  }

  // SQL query required
  if (!options.query || !options.query.value) {
    errors.push({ key: 'query', message: 'You must provide a SQL Query Template.' });
  }

  callback(null, errors);
};

module.exports = { startup, doLookup, onMessage, validateOptions };

'use strict';

const { generateJwt, isJwtExpired } = require('./src/generateJwt');
const { submitStatement, pollStatement, cancelStatement } = require('./src/querySnowflake');
const { mapResultRows, buildSummaryTags, parseAttributeList, parseErrorToReadableJSON } = require('./src/dataTransformations');
const { validateStringOptions, validateUrlOption } = require('./src/validateOptions');

let Logger;
// JWT cache: { token, expiresAt }
let jwtCache = null;

/**
 * Normalizes a Polarity option value regardless of how the server delivers it.
 *
 * Different Polarity server versions pass options in different shapes:
 *   - Plain value:               options.key  === 'the string'
 *   - Single-wrapped:            options.key  === { value: 'the string' }
 *   - Double-wrapped (selects):  options.key  === { value: { value: 'oauth', label: '...' } }
 *
 * This helper always returns the innermost scalar value (or '' if absent).
 */
function getOpt(options, key) {
  const raw = options[key];
  if (raw === null || raw === undefined) return '';
  if (typeof raw !== 'object') return raw; // plain string/number/boolean
  const v = raw.value;
  if (v !== null && v !== undefined && typeof v === 'object' && 'value' in v) return v.value; // double-wrapped
  return v ?? ''; // single-wrapped
}

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
  const authTypeRaw = options.authType;
  const authType = getOpt(options, 'authType');
  const oauthTokenRaw = options.oauthToken;
  const oauthTokenResolved = getOpt(options, 'oauthToken');

  Logger.info(
    {
      authTypeRaw: typeof authTypeRaw === 'object' ? JSON.stringify(authTypeRaw) : authTypeRaw,
      authTypeResolved: authType,
      authTypeMatch: authType === 'oauth',
      oauthTokenRawType: typeof oauthTokenRaw,
      oauthTokenRawIsObject: typeof oauthTokenRaw === 'object',
      oauthTokenRawObjectKeys: typeof oauthTokenRaw === 'object' && oauthTokenRaw ? Object.keys(oauthTokenRaw) : null,
      oauthTokenResolvedLength: oauthTokenResolved ? oauthTokenResolved.length : 0
    },
    'getToken diagnostics'
  );

  if (authType === 'oauth') {
    return oauthTokenResolved;
  }

  // Key-pair JWT — use cache unless expired
  if (jwtCache && !isJwtExpired(jwtCache.expiresAt)) {
    Logger.trace('Using cached JWT');
    return jwtCache.token;
  }

  Logger.debug('Generating new JWT for key-pair auth');
  const rawPrivateKey = getOpt(options, 'privateKey');
  const normalizedForDiag = (rawPrivateKey || '').replace(/\\n/g, '\n').replace(/\\r/g, '').trim();
  const pemHeader = normalizedForDiag.split('\n')[0] || '(empty)';
  Logger.info(
    {
      pemHeader,
      keyLength: normalizedForDiag.length,
      hasPassphrase: !!(getOpt(options, 'privateKeyPassphrase')),
      username: getOpt(options, 'username'),
      accountIdentifier: getOpt(options, 'accountIdentifier')
    },
    'getToken key-pair diagnostics'
  );
  const { token, expiresAt, fingerprint, jwtIss, jwtSub } = generateJwt({
    accountIdentifier: getOpt(options, 'accountIdentifier'),
    username: getOpt(options, 'username'),
    privateKey: getOpt(options, 'privateKey'),
    privateKeyPassphrase: getOpt(options, 'privateKeyPassphrase')
  });
  Logger.info(
    {
      fingerprint,
      jwtIss,
      jwtSub,
      hint: 'Run in Snowflake: DESCRIBE USER POLARITY_EXTSVC; and compare RSA_PUBLIC_KEY_FP value against fingerprint above.'
    },
    'JWT generated — verify fingerprint matches Snowflake'
  );
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
    const result = await pollStatement({ baseUrl, token, authType, statementHandle, logger: Logger });
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
  const matches = (query || '').match(/\?/g) || [];
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

  const baseUrl = buildBaseUrl(getOpt(options, 'accountIdentifier'));
  const authType = getOpt(options, 'authType') === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
  const bindingType = getOpt(options, 'bindingType');
  const query = getOpt(options, 'query');

  Logger.trace(
    {
      'options.query (raw)': options.query,
      queryResolved: query
    },
    'doLookup options.query diagnostic'
  );
  const queryTimeout = Number(getOpt(options, 'queryTimeout')) || 30;
  const resultLimit = Number(getOpt(options, 'resultLimit')) || 100;

  const summaryAttrList = parseAttributeList(getOpt(options, 'summaryAttributes'));
  const detailAttrList = parseAttributeList(getOpt(options, 'detailAttributes'));
  const itemTitleAttr = (getOpt(options, 'itemTitleAttribute') || '').trim().toUpperCase();
  const maxSummaryItems = Number(getOpt(options, 'maxSummaryItems')) || 3;

  Logger.trace(
    {
      baseUrl,
      authType,
      bindingType,
      query,
      queryTimeout,
      resultLimit,
      summaryAttrList,
      detailAttrList,
      itemTitleAttr,
      maxSummaryItems,
      tokenLength: token ? token.length : 0
    },
    'doLookup resolved options'
  );

  const lookupResults = await Promise.all(
    entities.map(async (entity) => {
      const startTime = Date.now();
      let statementHandle;

      try {
        if (!query) {
          Logger.warn({ entity: entity.value }, 'No SQL query configured — skipping lookup');
          return { entity, data: null };
        }
        const bindings = buildBindings(query, entity.value, bindingType);

        Logger.trace({ entity: entity.value, bindings }, 'Built bindings for entity');

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
        if (getOpt(options, 'warehouse')) body.warehouse = getOpt(options, 'warehouse');
        if (getOpt(options, 'database')) body.database = getOpt(options, 'database');
        if (getOpt(options, 'schema')) body.schema = getOpt(options, 'schema');
        if (getOpt(options, 'role')) body.role = getOpt(options, 'role');

        Logger.trace({ entity: entity.value, requestBody: body }, 'Submitting statement to Snowflake');

        const submitResult = await submitStatement({ baseUrl, token, authType, body, logger: Logger });

        Logger.trace(
          { entity: entity.value, status: submitResult.status, body: submitResult.body },
          'Statement submit response'
        );

        if (submitResult.status === 200) {
          const numRows = submitResult.body?.resultSetMetaData?.numRows;
          const columnNames = (submitResult.body?.resultSetMetaData?.rowType || []).map((c) => c.name);
          Logger.trace({ entity: entity.value, numRows, columnNames }, 'Synchronous result received');
          return buildLookupResult(entity, submitResult.body, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, Date.now() - startTime, Logger);
        }

        if (submitResult.status === 202) {
          // Async — poll
          statementHandle = submitResult.body.statementHandle;
          Logger.trace({ entity: entity.value, statementHandle }, 'Async execution — beginning poll');
          const pollResult = await pollToCompletion(statementHandle, baseUrl, token, authType, startTime);

          if (pollResult.complete) {
            const numRows = pollResult.resultSet?.resultSetMetaData?.numRows;
            const columnNames = (pollResult.resultSet?.resultSetMetaData?.rowType || []).map((c) => c.name);
            Logger.trace({ entity: entity.value, numRows, columnNames, elapsedMs: pollResult.elapsedMs }, 'Poll complete — result received');
            return buildLookupResult(entity, pollResult.resultSet, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, pollResult.elapsedMs, Logger);
          }

          Logger.trace({ entity: entity.value, statementHandle, elapsedMs: pollResult.elapsedMs }, 'Poll budget exhausted — returning pending state');
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

function buildLookupResult(entity, resultSet, summaryAttrList, detailAttrList, itemTitleAttr, maxSummaryItems, elapsedMs, Logger) {
  Logger.trace(
    {
      entity: entity.value,
      resultSetMetaData: resultSet?.resultSetMetaData,
      dataRowCount: resultSet?.data?.length ?? 0,
      statementHandle: resultSet?.statementHandle,
      message: resultSet?.message
    },
    'buildLookupResult — raw resultSet metadata'
  );

  const rows = mapResultRows(resultSet, detailAttrList, itemTitleAttr);

  Logger.trace(
    {
      entity: entity.value,
      mappedRowCount: rows.length,
      detailAttrList,
      itemTitleAttr,
      firstRow: rows[0] || null
    },
    'buildLookupResult — after mapResultRows'
  );

  if (rows.length === 0) {
    Logger.trace({ entity: entity.value }, 'buildLookupResult — 0 rows mapped → returning null (no overlay)');
    return { entity, data: null };
  }

  const summaryTags = buildSummaryTags(rows, summaryAttrList, maxSummaryItems);

  Logger.trace({ entity: entity.value, summaryTags }, 'buildLookupResult — summary tags built');
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

  const baseUrl = buildBaseUrl(getOpt(options, 'accountIdentifier'));
  const authType = getOpt(options, 'authType') === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
  const { statementHandle } = payload;

  try {
    const result = await pollStatement({ baseUrl, token, authType, statementHandle, logger: Logger });

    if (result.status === 200) {
      const summaryAttrList = parseAttributeList(getOpt(options, 'summaryAttributes'));
      const detailAttrList = parseAttributeList(getOpt(options, 'detailAttributes'));
      const itemTitleAttr = (getOpt(options, 'itemTitleAttribute') || '').trim().toUpperCase();
      const maxSummaryItems = Number(getOpt(options, 'maxSummaryItems')) || 3;

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

  if (!getOpt(options, 'accountIdentifier')) {
    errors.push({ key: 'accountIdentifier', message: 'You must provide a Snowflake Account Identifier.' });
  }

  const authType = getOpt(options, 'authType');
  if (authType === 'oauth') {
    if (!getOpt(options, 'oauthToken')) {
      errors.push({ key: 'oauthToken', message: 'An OAuth Token is required when Authentication Type is set to "OAuth Token".' });
    }
  } else if (authType === 'keypair') {
    if (!getOpt(options, 'username')) {
      errors.push({ key: 'username', message: 'A Username is required for Key-Pair JWT authentication.' });
    }
    if (!getOpt(options, 'privateKey')) {
      errors.push({ key: 'privateKey', message: 'A Private Key (PEM) is required for Key-Pair JWT authentication.' });
    }
  }

  if (!getOpt(options, 'query')) {
    errors.push({ key: 'query', message: 'You must provide a SQL Query Template.' });
  }

  callback(null, errors);
};

module.exports = { startup, doLookup, onMessage, validateOptions };

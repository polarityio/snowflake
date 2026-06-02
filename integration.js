'use strict';

const { generateJwt, isJwtExpired } = require('./src/generateJwt');
const {
  submitStatement,
  pollStatement,
  cancelStatement
} = require('./src/querySnowflake');
const {
  mapResultRows,
  buildSummaryTags,
  parseAttributeList,
  parseErrorToReadableJSON
} = require('./src/dataTransformations');
const { validateConnectivity } = require('./src/validateOptions');

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
 * Returns a valid bearer token for the configured auth type.
 * For OAuth: returns options.oauthToken directly.
 * For Key-Pair JWT: generates a fresh JWT or returns the cached one if still valid.
 *
 * NOTE: All diagnostic logging here is at TRACE level. Polarity's TRACE logs
 * are scoped to admin debugging sessions and are not written to standard logs.
 * Token shape, fingerprint, IDP claims, username, and account identifier
 * must NEVER appear at INFO or DEBUG level.
 */
async function getToken(options) {
  const authType = getOpt(options, 'authType');

  Logger.trace(
    {
      authTypeResolved: authType,
      hasOauthToken: !!getOpt(options, 'oauthToken'),
      hasPrivateKey: !!getOpt(options, 'privateKey')
    },
    'getToken — resolved auth shape'
  );

  if (authType === 'oauth') {
    return getOpt(options, 'oauthToken');
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

  Logger.trace(
    {
      pemHeader,
      keyLength: normalizedForDiag.length,
      hasPassphrase: !!(getOpt(options, 'privateKeyPassphrase')),
      username: getOpt(options, 'username'),
      accountIdentifier: getOpt(options, 'accountIdentifier')
    },
    'getToken — key-pair input diagnostics (TRACE only)'
  );

  const { token, expiresAt, fingerprint, jwtIss, jwtSub } = generateJwt({
    accountIdentifier: getOpt(options, 'accountIdentifier'),
    username: getOpt(options, 'username'),
    privateKey: getOpt(options, 'privateKey'),
    privateKeyPassphrase: getOpt(options, 'privateKeyPassphrase')
  });

  // PII-safe INFO breadcrumb — confirms successful generation without leaking
  // identifiers, fingerprints, or claim values.
  Logger.info({ tokenLength: token.length }, 'JWT generated successfully');

  // Detailed claim/fingerprint info kept at TRACE for support debugging only.
  Logger.trace(
    {
      fingerprint,
      jwtIss,
      jwtSub,
      hint: 'Run in Snowflake: DESCRIBE USER <SERVICE_USER>; and compare RSA_PUBLIC_KEY_FP value against fingerprint above.'
    },
    'JWT claim diagnostics (TRACE only)'
  );

  jwtCache = { token, expiresAt };
  return token;
}

/**
 * Polls a pending statement handle until complete or max attempts reached.
 *
 * On success: returns { complete: true, resultSet, elapsedMs }.
 *
 * On poll-budget exhaustion: cancels the statement on the server (best-effort)
 * and throws a structured Error with userMessage + isTimeout = true. Per spec
 * this is a hard timeout — we do NOT return a "still running" placeholder
 * because that placed the burden of reconciliation on the user.
 *
 * Warehouse-suspended responses extend the poll budget by +3 attempts and
 * surface a `warehouseWaking` flag for the UI.
 */
async function pollToCompletion({ baseUrl, token, authType, statementHandle, startTime }) {
  let extraAttempts = 0;
  let warehouseWaking = false;

  for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS + extraAttempts; attempt++) {
    const interval = POLL_INTERVALS_MS[Math.min(attempt, POLL_INTERVALS_MS.length - 1)];
    await sleep(interval);

    let result;
    try {
      result = await pollStatement({ baseUrl, token, authType, statementHandle, logger: Logger });
    } catch (err) {
      if (err.isWarehouseError && extraAttempts === 0) {
        // First time we detect the warehouse is resuming — extend the budget once.
        warehouseWaking = true;
        extraAttempts = 3;
        Logger.info(
          { statementHandle, attempt },
          'Warehouse is resuming — extending poll budget by 3 attempts'
        );
        continue;
      }
      throw err;
    }

    if (result.status === 200) {
      return {
        complete: true,
        resultSet: result.body,
        elapsedMs: Date.now() - startTime,
        warehouseWaking
      };
    }
    Logger.trace({ attempt, statementHandle }, 'Query still running — retrying poll');
  }

  // Poll budget exhausted — cancel on the server (best-effort) and throw.
  Logger.warn(
    { statementHandle, attempts: MAX_POLL_ATTEMPTS + extraAttempts, elapsedMs: Date.now() - startTime },
    'Poll budget exhausted — cancelling statement on Snowflake'
  );

  try {
    await cancelStatement({ baseUrl, token, authType, statementHandle, logger: Logger });
  } catch (cancelErr) {
    Logger.warn(
      { err: parseErrorToReadableJSON(cancelErr), statementHandle },
      'Cancel-on-timeout call failed (non-fatal)'
    );
  }

  const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
  const err = new Error(
    `Query exceeded poll budget (${MAX_POLL_ATTEMPTS + extraAttempts} attempts, ~${elapsedSec}s) and was cancelled.`
  );
  err.isTimeout = true;
  err.statementHandle = statementHandle;
  err.warehouseWaking = warehouseWaking;
  err.userMessage =
    'Query timed out and was cancelled on the server. Increase Query Timeout, narrow the result set, or simplify the SQL.';
  throw err;
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
    return cb({
      detail: 'Authentication failed — check credentials in integration settings.',
      err: readable
    });
  }

  const baseUrl = buildBaseUrl(getOpt(options, 'accountIdentifier'));
  const authType = getOpt(options, 'authType') === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
  const bindingType = getOpt(options, 'bindingType');
  const query = getOpt(options, 'query');

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
      queryLength: (query || '').length,
      queryTimeout,
      resultLimit,
      summaryAttrList,
      detailAttrList,
      itemTitleAttr,
      maxSummaryItems,
      tokenLength: token ? token.length : 0
    },
    'doLookup — resolved options'
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

        Logger.trace({ entity: entity.value }, 'Submitting statement to Snowflake');

        const submitResult = await submitStatement({ baseUrl, token, authType, body, logger: Logger });

        Logger.trace(
          { entity: entity.value, status: submitResult.status, requestId: submitResult.requestId },
          'Statement submit response'
        );

        if (submitResult.status === 200) {
          return buildLookupResult(
            entity,
            submitResult.body,
            summaryAttrList,
            detailAttrList,
            itemTitleAttr,
            maxSummaryItems,
            Date.now() - startTime,
            { warehouseWaking: false },
            query,
            Logger
          );
        }

        if (submitResult.status === 202) {
          // Async — poll until complete or budget exhausted (with cancel-on-exhaust).
          statementHandle = submitResult.body.statementHandle;
          Logger.trace({ entity: entity.value, statementHandle }, 'Async execution — beginning poll');

          const pollResult = await pollToCompletion({
            baseUrl,
            token,
            authType,
            statementHandle,
            startTime
          });

          // pollToCompletion always either returns complete=true or throws on exhaustion.
          return buildLookupResult(
            entity,
            pollResult.resultSet,
            summaryAttrList,
            detailAttrList,
            itemTitleAttr,
            maxSummaryItems,
            pollResult.elapsedMs,
            { warehouseWaking: pollResult.warehouseWaking },
            query,
            Logger
          );
        }

        // Unexpected status from submit
        Logger.error(
          { status: submitResult.status, body: submitResult.body },
          'Unexpected submit status'
        );
        return buildErrorResult(
          entity,
          `Unexpected response status ${submitResult.status} from Snowflake.`,
          {}
        );
      } catch (err) {
        const readable = parseErrorToReadableJSON(err);
        Logger.error({ entity: entity.value, err: readable }, 'Entity lookup error');
        return buildErrorResult(entity, err.userMessage || err.message || 'Lookup failed', {
          isTimeout: !!err.isTimeout,
          isAuthError: !!err.isAuthError,
          isWarehouseError: !!err.isWarehouseError,
          warehouseWaking: !!err.warehouseWaking,
          statementHandle: err.statementHandle || statementHandle || ''
        });
      }
    })
  );

  Logger.trace({ resultCount: lookupResults.length }, 'Lookup Results');
  cb(null, lookupResults);
};

function buildLookupResult(
  entity,
  resultSet,
  summaryAttrList,
  detailAttrList,
  itemTitleAttr,
  maxSummaryItems,
  elapsedMs,
  flags,
  renderedQuery,
  Logger
) {
  Logger.trace(
    {
      entity: entity.value,
      dataRowCount: resultSet?.data?.length ?? 0,
      statementHandle: resultSet?.statementHandle
    },
    'buildLookupResult — raw resultSet metadata'
  );

  const rows = mapResultRows(resultSet, detailAttrList, itemTitleAttr);

  if (rows.length === 0) {
    Logger.trace({ entity: entity.value }, 'buildLookupResult — 0 rows mapped → returning null (no overlay)');
    return { entity, data: null };
  }

  const summaryTags = buildSummaryTags(rows, summaryAttrList, maxSummaryItems);
  if (flags?.warehouseWaking) summaryTags.unshift('❄️ Warehouse Resumed');

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
        renderedQuery: renderedQuery || '',
        warehouseWaking: !!flags?.warehouseWaking,
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

function buildErrorResult(entity, message, flags) {
  // Distinct summary tag per error class so analysts can scan a list of entities
  // and instantly see what's broken.
  let summaryTag = '⚠ Query Error';
  if (flags?.isTimeout) summaryTag = '⏱ Query Cancelled';
  else if (flags?.isAuthError) summaryTag = '🔒 Auth Failed';
  else if (flags?.isWarehouseError) summaryTag = '❄️ Warehouse Suspended';

  return {
    entity,
    data: {
      summary: [summaryTag],
      details: {
        complete: true,
        isError: true,
        isTimeout: !!flags?.isTimeout,
        isAuthError: !!flags?.isAuthError,
        isWarehouseError: !!flags?.isWarehouseError,
        warehouseWaking: !!flags?.warehouseWaking,
        statementHandle: flags?.statementHandle || '',
        errorMessage: message,
        results: []
      }
    }
  };
}

const validateOptions = async (options, callback) => {
  const errors = [];

  const accountIdentifier = getOpt(options, 'accountIdentifier');
  if (!accountIdentifier) {
    errors.push({
      key: 'accountIdentifier',
      message: 'You must provide a Snowflake Account Identifier.'
    });
  }

  const authType = getOpt(options, 'authType');
  if (authType === 'oauth') {
    if (!getOpt(options, 'oauthToken')) {
      errors.push({
        key: 'oauthToken',
        message: 'An OAuth Token is required when Authentication Type is set to "OAuth Token".'
      });
    }
  } else if (authType === 'keypair') {
    if (!getOpt(options, 'username')) {
      errors.push({ key: 'username', message: 'A Username is required for Key-Pair JWT authentication.' });
    }
    if (!getOpt(options, 'privateKey')) {
      errors.push({
        key: 'privateKey',
        message: 'A Private Key (PEM) is required for Key-Pair JWT authentication.'
      });
    }
  }

  if (!getOpt(options, 'query')) {
    errors.push({ key: 'query', message: 'You must provide a SQL Query Template.' });
  }

  // If field-level validation failed, don't bother probing — return errors now.
  if (errors.length > 0) {
    return callback(null, errors);
  }

  // Connectivity probe — verifies the base URL resolves and creds are valid.
  // Best-effort: only adds errors when the failure is unambiguous (auth or wrong host).
  try {
    const probeToken = await getToken(options);
    const baseUrl = buildBaseUrl(accountIdentifier);
    const probeAuthType = authType === 'oauth' ? 'OAUTH' : 'KEYPAIR_JWT';
    const probeErrors = await validateConnectivity({
      baseUrl,
      token: probeToken,
      authType: probeAuthType,
      logger: Logger
    });
    errors.push(...probeErrors);
  } catch (err) {
    // Token generation failed (e.g., bad PEM). Surface as a privateKey error.
    Logger.warn({ err: parseErrorToReadableJSON(err) }, 'validateOptions — token generation failed during probe');
    errors.push({
      key: authType === 'oauth' ? 'oauthToken' : 'privateKey',
      message: `Could not authenticate: ${err.message || 'Unknown error'}`
    });
  }

  callback(null, errors);
};

module.exports = { startup, doLookup, validateOptions };

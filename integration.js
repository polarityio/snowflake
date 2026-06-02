'use strict';

const crypto = require('crypto');
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
 * Module-scoped result cache. Keys are SHA-256 hex digests of the canonical
 * cache key string. Values are { result, expiresAt } where `result` is the
 * fully-formatted lookup-result object as returned to Polarity (sans `entity`).
 */
const lookupCache = new Map();

/**
 * Detects non-deterministic SQL functions whose presence makes any cached
 * result stale immediately. When the rendered query matches this regex the
 * cache is bypassed entirely — neither read nor written.
 */
const NON_DETERMINISTIC_RE =
  /\b(now|current_timestamp|current_date|current_time|sysdate|getdate|random|uuid_string)\s*\(/i;

const MAX_PARTITIONS_HARD_CAP = 10;

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

/**
 * Counts the number of ? placeholders in a SQL string.
 */
function countPlaceholders(sql) {
  return ((sql || '').match(/\?/g) || []).length;
}

/**
 * Inspects the rendered SQL and returns the column name used in the entity
 * predicate when the SQL is bulk-rewritable, or `null` when it is not.
 *
 * Bulk-rewritable forms (single ? placeholder, single predicate):
 *   col = ?                 → rewritten to  col IN (?, ?, ...)
 *   col IN (?)              → rewritten to  col IN (?, ?, ...)
 *   schema.tbl.col = ?      → column is `col` (last dot segment)
 *
 * The match must be unambiguous — if the SQL contains either form more than
 * once, or the lone ? is NOT inside one of these forms, the SQL is not
 * bulk-rewritable.
 */
function detectBulkColumn(sql) {
  if (!sql || typeof sql !== 'string') return null;
  if (countPlaceholders(sql) !== 1) return null;

  const eqRe = /(\w+(?:\.\w+)*)\s*=\s*\?/g;
  const inRe = /(\w+(?:\.\w+)*)\s+IN\s*\(\s*\?\s*\)/gi;

  const eqMatches = [...sql.matchAll(eqRe)];
  const inMatches = [...sql.matchAll(inRe)];

  const total = eqMatches.length + inMatches.length;
  if (total !== 1) return null; // 0 or >1 candidate predicates → fall back

  const match = (eqMatches[0] || inMatches[0])[1];
  if (!match) return null;
  const segments = match.split('.');
  const colName = segments[segments.length - 1];
  return {
    qualifiedName: match,
    columnName: colName,
    form: eqMatches.length === 1 ? 'eq' : 'in'
  };
}

/**
 * Rewrites a single-? bulk-rewritable SQL into one with N comma-separated
 * placeholders inside an IN(...) clause.
 *
 *   "SELECT ... WHERE ip = ?"          + N=3  →  "SELECT ... WHERE ip IN (?, ?, ?)"
 *   "SELECT ... WHERE ip IN (?)"       + N=3  →  "SELECT ... WHERE ip IN (?, ?, ?)"
 */
function rewriteSqlForBulk(sql, predicate, n) {
  const placeholders = new Array(n).fill('?').join(', ');
  if (predicate.form === 'eq') {
    const eqRe = /(\w+(?:\.\w+)*)\s*=\s*\?/;
    return sql.replace(eqRe, `${predicate.qualifiedName} IN (${placeholders})`);
  }
  // form === 'in'
  const inRe = /(\w+(?:\.\w+)*)\s+IN\s*\(\s*\?\s*\)/i;
  return sql.replace(inRe, `${predicate.qualifiedName} IN (${placeholders})`);
}

/**
 * Builds bindings for a bulk query. Each entity value gets one positional binding.
 */
function buildBulkBindings(entityValues, bindingType) {
  const bindings = {};
  entityValues.forEach((v, i) => {
    bindings[String(i + 1)] = { type: bindingType, value: v };
  });
  return bindings;
}

/**
 * Locates the result-column index that matches the bulk predicate column,
 * case-insensitively. Returns -1 if not present in the resultSetMetaData.
 */
function findResultColumnIndex(resultSet, columnName) {
  const cols = resultSet?.resultSetMetaData?.rowType || [];
  const target = (columnName || '').toUpperCase();
  for (let i = 0; i < cols.length; i++) {
    if ((cols[i].name || '').toUpperCase() === target) return i;
  }
  return -1;
}

/**
 * Splits a bulk resultSet's `data` rows into per-entity sub-resultSets keyed
 * by the lower-cased entity value. Each sub-resultSet is a shallow copy with
 * its own filtered `data` array — `resultSetMetaData` is shared.
 *
 * Rows whose split key is null/empty are dropped from the per-entity buckets
 * (they cannot be attributed to a specific entity).
 */
function splitResultsByEntity(resultSet, entityValues, columnIndex) {
  const buckets = new Map();
  const knownKeys = new Set(entityValues.map((v) => String(v).toLowerCase()));
  entityValues.forEach((v) => buckets.set(String(v).toLowerCase(), []));

  const data = Array.isArray(resultSet?.data) ? resultSet.data : [];
  for (const row of data) {
    if (columnIndex < 0 || columnIndex >= row.length) continue;
    const cellValue = row[columnIndex];
    if (cellValue === null || cellValue === undefined) continue;
    const key = String(cellValue).toLowerCase();
    if (knownKeys.has(key)) {
      buckets.get(key).push(row);
    }
  }

  const subResultSets = {};
  for (const [key, rows] of buckets.entries()) {
    subResultSets[key] = {
      ...resultSet,
      data: rows
    };
  }
  return subResultSets;
}

/**
 * Fetches additional result-set partitions (1..N) and concatenates the data
 * arrays into the partition-0 resultSet. Returns the merged resultSet and
 * the number of partitions actually fetched.
 *
 * Snowflake splits large result sets into ~10MB partitions; partition 0 is
 * delivered with the initial completion response, and additional partitions
 * are pulled serially via /api/v2/statements/<handle>?partition=N.
 *
 * `maxPartitions` is the user-configured cap (1..MAX_PARTITIONS_HARD_CAP).
 */
async function fetchAdditionalPartitions({
  baseUrl,
  token,
  authType,
  resultSet,
  maxPartitions
}) {
  const partitionInfo = resultSet?.resultSetMetaData?.partitionInfo || [];
  const totalPartitions = partitionInfo.length || 1;
  const cap = Math.max(1, Math.min(Number(maxPartitions) || 1, MAX_PARTITIONS_HARD_CAP));
  const targetCount = Math.min(totalPartitions, cap);

  if (targetCount <= 1 || totalPartitions <= 1) {
    return {
      mergedResultSet: resultSet,
      partitionsFetched: 1,
      partitionsTotal: totalPartitions
    };
  }

  const handle = resultSet.statementHandle;
  const mergedData = Array.isArray(resultSet.data) ? [...resultSet.data] : [];

  for (let p = 1; p < targetCount; p++) {
    try {
      const part = await pollStatement({
        baseUrl,
        token,
        authType,
        statementHandle: handle,
        partition: p,
        logger: Logger
      });
      if (part.status === 200 && Array.isArray(part.body?.data)) {
        for (const row of part.body.data) mergedData.push(row);
      } else {
        Logger.warn(
          { statementHandle: handle, partition: p, status: part.status },
          'Unexpected status fetching additional partition — stopping partition merge'
        );
        return {
          mergedResultSet: { ...resultSet, data: mergedData },
          partitionsFetched: p,
          partitionsTotal: totalPartitions
        };
      }
    } catch (err) {
      Logger.warn(
        { statementHandle: handle, partition: p, err: err.message },
        'Error fetching additional partition — returning rows fetched so far'
      );
      return {
        mergedResultSet: { ...resultSet, data: mergedData },
        partitionsFetched: p,
        partitionsTotal: totalPartitions
      };
    }
  }

  if (totalPartitions > cap) {
    Logger.warn(
      { statementHandle: handle, totalPartitions, cap },
      'Result has more partitions than maxPartitions cap — truncating'
    );
  }

  return {
    mergedResultSet: { ...resultSet, data: mergedData },
    partitionsFetched: targetCount,
    partitionsTotal: totalPartitions
  };
}

/**
 * Builds a SHA-256 cache key from the rendered SQL plus context that affects
 * the result (entity value, warehouse, role, database, schema). The exact
 * authentication identity is intentionally NOT part of the key — caching is
 * per integration instance, not per Polarity user.
 */
function makeCacheKey({ sql, entityValue, warehouse, role, database, schema }) {
  const parts = [
    sql || '',
    String(entityValue ?? ''),
    warehouse || '',
    role || '',
    database || '',
    schema || ''
  ];
  return crypto.createHash('sha256').update(parts.join('\u0000')).digest('hex');
}

/**
 * Lazily-evicting cache get. Returns the cached lookup-result object if fresh,
 * or null on miss / expired entry. Expired entries are deleted on read.
 */
function cacheGet(key) {
  const entry = lookupCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    lookupCache.delete(key);
    return null;
  }
  return entry.result;
}

function cacheSet(key, result, ttlSeconds) {
  if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) return;
  lookupCache.set(key, { result, expiresAt: Date.now() + ttlSeconds * 1000 });
}

/**
 * True iff the given SQL contains any non-deterministic function call that
 * disqualifies the result from being cached.
 */
function isNonDeterministic(sql) {
  return NON_DETERMINISTIC_RE.test(sql || '');
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

  const warehouseOpt = getOpt(options, 'warehouse');
  const databaseOpt = getOpt(options, 'database');
  const schemaOpt = getOpt(options, 'schema');
  const roleOpt = getOpt(options, 'role');

  // New behavior toggles (default true / 1 / 300 to match documented defaults).
  const bulkLookupEnabled = getOpt(options, 'bulkLookupEnabled') !== false;
  const maxPartitionsRaw = Number(getOpt(options, 'maxPartitions'));
  const maxPartitions = Math.max(
    1,
    Math.min(
      Number.isFinite(maxPartitionsRaw) && maxPartitionsRaw > 0 ? maxPartitionsRaw : 1,
      MAX_PARTITIONS_HARD_CAP
    )
  );
  const cacheEnabled = getOpt(options, 'cacheEnabled') !== false;
  const cacheTtlRaw = Number(getOpt(options, 'cacheTtlSeconds'));
  const cacheTtl = Number.isFinite(cacheTtlRaw) && cacheTtlRaw >= 0 ? cacheTtlRaw : 300;
  const cacheBypassed = isNonDeterministic(query);
  const cacheActive = cacheEnabled && cacheTtl > 0 && !cacheBypassed;

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
      bulkLookupEnabled,
      maxPartitions,
      cacheEnabled,
      cacheTtl,
      cacheBypassed,
      tokenLength: token ? token.length : 0
    },
    'doLookup — resolved options'
  );

  if (!query) {
    Logger.warn('No SQL query configured — skipping all lookups');
    return cb(null, entities.map((e) => ({ entity: e, data: null })));
  }

  // ── Step 1: cache lookup pass ────────────────────────────────────────────
  // For each entity, build a cache key. If the query is non-deterministic or
  // caching is disabled, every entity is treated as a miss.
  const slots = entities.map((entity) => {
    const cacheKey = cacheActive
      ? makeCacheKey({
          sql: query,
          entityValue: entity.value,
          warehouse: warehouseOpt,
          role: roleOpt,
          database: databaseOpt,
          schema: schemaOpt
        })
      : null;
    const cached = cacheKey ? cacheGet(cacheKey) : null;
    return { entity, cacheKey, cached, result: cached || null };
  });

  const cacheHits = slots.filter((s) => s.cached).length;
  const misses = slots.filter((s) => !s.cached);
  Logger.trace(
    { totalEntities: entities.length, cacheHits, cacheMisses: misses.length, cacheActive },
    'doLookup — cache pass complete'
  );

  // ── Step 2: dispatch — bulk vs per-entity ────────────────────────────────
  const predicate = detectBulkColumn(query);
  const canBulk =
    bulkLookupEnabled &&
    misses.length > 1 &&
    countPlaceholders(query) === 1 &&
    predicate !== null;

  if (canBulk) {
    Logger.trace(
      { predicate, missCount: misses.length },
      'doLookup — using bulk-lookup path (single rewritten query)'
    );
    await runBulkLookup({
      missSlots: misses,
      query,
      predicate,
      baseUrl,
      token,
      authType,
      bindingType,
      body: {
        timeout: queryTimeout,
        parameters: {
          rows_per_resultset: resultLimit,
          query_tag: 'polarity-integration',
          use_cached_result: 'true'
        },
        warehouse: warehouseOpt,
        database: databaseOpt,
        schema: schemaOpt,
        role: roleOpt
      },
      summaryAttrList,
      detailAttrList,
      itemTitleAttr,
      maxSummaryItems,
      maxPartitions
    });
  } else {
    if (bulkLookupEnabled && misses.length > 1 && !canBulk) {
      Logger.info(
        { hasPredicate: predicate !== null, placeholderCount: countPlaceholders(query) },
        'doLookup — bulk-lookup not applicable (no single ?-predicate detected); using per-entity fallback'
      );
    }
    await Promise.all(
      misses.map(async (slot) => {
        slot.result = await runSingleEntityLookup({
          entity: slot.entity,
          query,
          baseUrl,
          token,
          authType,
          bindingType,
          warehouse: warehouseOpt,
          database: databaseOpt,
          schema: schemaOpt,
          role: roleOpt,
          queryTimeout,
          resultLimit,
          summaryAttrList,
          detailAttrList,
          itemTitleAttr,
          maxSummaryItems,
          maxPartitions
        });
      })
    );
  }

  // ── Step 3: write back to cache ──────────────────────────────────────────
  if (cacheActive) {
    for (const slot of slots) {
      if (slot.cached) continue; // already a hit
      if (!slot.result || !slot.result.data) continue; // null / no-data
      if (slot.result.data?.details?.isError) continue; // never cache errors
      cacheSet(slot.cacheKey, slot.result, cacheTtl);
    }
  }

  const lookupResults = slots.map((s) => s.result || { entity: s.entity, data: null });
  Logger.trace({ resultCount: lookupResults.length, cacheHits }, 'Lookup Results');
  cb(null, lookupResults);
};

/**
 * Per-entity lookup path — submit, optionally poll, fetch additional partitions,
 * format. Returns a Polarity lookup-result object (never throws).
 */
async function runSingleEntityLookup({
  entity,
  query,
  baseUrl,
  token,
  authType,
  bindingType,
  warehouse,
  database,
  schema,
  role,
  queryTimeout,
  resultLimit,
  summaryAttrList,
  detailAttrList,
  itemTitleAttr,
  maxSummaryItems,
  maxPartitions
}) {
  const startTime = Date.now();
  let statementHandle;

  try {
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
    if (warehouse) body.warehouse = warehouse;
    if (database) body.database = database;
    if (schema) body.schema = schema;
    if (role) body.role = role;

    const submitResult = await submitStatement({ baseUrl, token, authType, body, logger: Logger });
    Logger.trace(
      { entity: entity.value, status: submitResult.status, requestId: submitResult.requestId },
      'Statement submit response'
    );

    let resultSet;
    let warehouseWaking = false;
    let elapsedMs;

    if (submitResult.status === 200) {
      resultSet = submitResult.body;
      elapsedMs = Date.now() - startTime;
    } else if (submitResult.status === 202) {
      statementHandle = submitResult.body.statementHandle;
      const pollResult = await pollToCompletion({
        baseUrl,
        token,
        authType,
        statementHandle,
        startTime
      });
      resultSet = pollResult.resultSet;
      warehouseWaking = pollResult.warehouseWaking;
      elapsedMs = pollResult.elapsedMs;
    } else {
      Logger.error(
        { status: submitResult.status, body: submitResult.body },
        'Unexpected submit status'
      );
      return buildErrorResult(
        entity,
        `Unexpected response status ${submitResult.status} from Snowflake.`,
        {}
      );
    }

    // Multi-partition fetch (no-op when maxPartitions=1 or only 1 partition exists).
    const partitionResult = await fetchAdditionalPartitions({
      baseUrl,
      token,
      authType,
      resultSet,
      maxPartitions
    });

    return buildLookupResult(
      entity,
      partitionResult.mergedResultSet,
      summaryAttrList,
      detailAttrList,
      itemTitleAttr,
      maxSummaryItems,
      elapsedMs,
      { warehouseWaking },
      query,
      Logger,
      partitionResult.partitionsFetched,
      partitionResult.partitionsTotal
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
}

/**
 * Bulk-lookup path — rewrite SQL into IN(?,?,...), run a single Snowflake query,
 * fetch additional partitions, then split rows by predicate column to populate
 * each `missSlot.result`. Falls back to per-entity execution if the bulk query
 * fails for any reason.
 */
async function runBulkLookup({
  missSlots,
  query,
  predicate,
  baseUrl,
  token,
  authType,
  bindingType,
  body: bodyDefaults,
  summaryAttrList,
  detailAttrList,
  itemTitleAttr,
  maxSummaryItems,
  maxPartitions
}) {
  const entityValues = missSlots.map((s) => s.entity.value);
  const rewrittenSql = rewriteSqlForBulk(query, predicate, entityValues.length);
  const bindings = buildBulkBindings(entityValues, bindingType);
  const startTime = Date.now();

  const body = {
    statement: rewrittenSql,
    timeout: bodyDefaults.timeout,
    parameters: bodyDefaults.parameters,
    bindings
  };
  if (bodyDefaults.warehouse) body.warehouse = bodyDefaults.warehouse;
  if (bodyDefaults.database) body.database = bodyDefaults.database;
  if (bodyDefaults.schema) body.schema = bodyDefaults.schema;
  if (bodyDefaults.role) body.role = bodyDefaults.role;

  let resultSet;
  let warehouseWaking = false;
  let elapsedMs;
  let statementHandle;

  try {
    Logger.trace(
      {
        entityCount: entityValues.length,
        predicate: predicate.qualifiedName,
        rewrittenSqlLength: rewrittenSql.length
      },
      'runBulkLookup — submitting rewritten bulk query'
    );

    const submitResult = await submitStatement({ baseUrl, token, authType, body, logger: Logger });

    if (submitResult.status === 200) {
      resultSet = submitResult.body;
      elapsedMs = Date.now() - startTime;
    } else if (submitResult.status === 202) {
      statementHandle = submitResult.body.statementHandle;
      const pollResult = await pollToCompletion({
        baseUrl,
        token,
        authType,
        statementHandle,
        startTime
      });
      resultSet = pollResult.resultSet;
      warehouseWaking = pollResult.warehouseWaking;
      elapsedMs = pollResult.elapsedMs;
    } else {
      throw new Error(`Unexpected response status ${submitResult.status} from Snowflake.`);
    }
  } catch (err) {
    Logger.warn(
      { err: err.message, entityCount: entityValues.length },
      'Bulk lookup failed — falling back to per-entity queries'
    );
    // Fallback: run each miss as a single-entity lookup.
    await Promise.all(
      missSlots.map(async (slot) => {
        slot.result = await runSingleEntityLookup({
          entity: slot.entity,
          query,
          baseUrl,
          token,
          authType,
          bindingType,
          warehouse: bodyDefaults.warehouse,
          database: bodyDefaults.database,
          schema: bodyDefaults.schema,
          role: bodyDefaults.role,
          queryTimeout: bodyDefaults.timeout,
          resultLimit: bodyDefaults.parameters?.rows_per_resultset || 100,
          summaryAttrList,
          detailAttrList,
          itemTitleAttr,
          maxSummaryItems,
          maxPartitions
        });
      })
    );
    return;
  }

  // Multi-partition merge for bulk results.
  const partitionResult = await fetchAdditionalPartitions({
    baseUrl,
    token,
    authType,
    resultSet,
    maxPartitions
  });
  resultSet = partitionResult.mergedResultSet;

  // Split rows by predicate column and build per-entity results.
  const colIdx = findResultColumnIndex(resultSet, predicate.columnName);
  if (colIdx < 0) {
    Logger.warn(
      { columnName: predicate.columnName },
      'Bulk predicate column not present in result set — falling back to per-entity queries. ' +
        'Add the predicate column to the SELECT list to enable bulk lookups.'
    );
    await Promise.all(
      missSlots.map(async (slot) => {
        slot.result = await runSingleEntityLookup({
          entity: slot.entity,
          query,
          baseUrl,
          token,
          authType,
          bindingType,
          warehouse: bodyDefaults.warehouse,
          database: bodyDefaults.database,
          schema: bodyDefaults.schema,
          role: bodyDefaults.role,
          queryTimeout: bodyDefaults.timeout,
          resultLimit: bodyDefaults.parameters?.rows_per_resultset || 100,
          summaryAttrList,
          detailAttrList,
          itemTitleAttr,
          maxSummaryItems,
          maxPartitions
        });
      })
    );
    return;
  }

  const subResultSets = splitResultsByEntity(resultSet, entityValues, colIdx);

  for (const slot of missSlots) {
    const key = String(slot.entity.value).toLowerCase();
    const subRs = subResultSets[key] || { ...resultSet, data: [] };
    slot.result = buildLookupResult(
      slot.entity,
      subRs,
      summaryAttrList,
      detailAttrList,
      itemTitleAttr,
      maxSummaryItems,
      elapsedMs,
      { warehouseWaking },
      rewrittenSql,
      Logger,
      partitionResult.partitionsFetched,
      partitionResult.partitionsTotal
    );
  }
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
  Logger,
  partitionsFetched,
  partitionsTotal
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
  const partitionInfoCount = (resultSetMetaData?.partitionInfo || []).length || 1;
  const partitionsTotalEffective =
    typeof partitionsTotal === 'number' && partitionsTotal > 0 ? partitionsTotal : partitionInfoCount;
  const partitionsFetchedEffective =
    typeof partitionsFetched === 'number' && partitionsFetched > 0 ? partitionsFetched : 1;
  const isTruncated = partitionsFetchedEffective < partitionsTotalEffective;

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
          partitionCount: partitionsTotalEffective,
          partitionsFetched: partitionsFetchedEffective,
          partitionsTotal: partitionsTotalEffective,
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

# Changelog

All notable changes to the Polarity Snowflake integration are documented in this file.

## 1.1.0 — INT-553 deferred items closeout

Closes the three INT-553 audit items deferred from `1.0.0`. No breaking changes; all new behavior is opt-in via new admin-only options that default to safe values that match prior behavior (with the exception of bulk lookup, which is enabled by default but transparently falls back when the configured SQL doesn't match the bulk pattern).

### Added

- **Bulk lookup via `IN (?, ?, ...)` parameter expansion.** When `bulkLookupEnabled` is true (default) and the SQL Query Template uses a single `?` placeholder in either an `= ?` or `IN (?)` predicate, multiple entities in the same Polarity lookup batch are coalesced into one Snowflake query. Result rows are mapped back to each entity by matching the column referenced in the predicate, case-insensitively. Drops a 100-entity hover from N round-trips down to 1 for SQL that fits the pattern. SQL that does not fit the pattern (or has multiple `?` placeholders) falls back to per-entity execution with a single info-level log line. New option: `bulkLookupEnabled` (boolean, default `true`).
- **Multi-partition result fetch.** Snowflake splits large result sets into ~10MB partitions; the integration now serially pulls additional partitions via `/api/v2/statements/<handle>?partition=N` up to the configured cap, then concatenates the rows into a single `details.results` array. The truncation banner now displays `N of M partitions` so analysts can see how much was retrieved vs how much exists. New option: `maxPartitions` (number, default `1`, hard cap `10`). Default of `1` preserves prior behavior on upgrade.
- **In-memory result-level TTL cache.** Lookup results are cached in process memory keyed by SHA-256 of `(rendered SQL + entity value + warehouse + role + database + schema)`. Lazy eviction on read. Errors are never cached. The cache is automatically bypassed when the SQL contains any of the non-deterministic functions `now()`, `current_timestamp()`, `current_date()`, `current_time()`, `sysdate()`, `getdate()`, `random()`, or `uuid_string()` — neither read nor written. New options: `cacheEnabled` (boolean, default `true`), `cacheTtlSeconds` (number, default `300`).
- **`details.executionStats.partitionsFetched`** and **`partitionsTotal`** fields on every result, surfaced through `reducers/details.json` and visible in the AI assistant's reduced view.

### Changed

- **Truncation banner copy** now reads `Showing N of M result partitions — increase Max Result Partitions, raise the Result Limit, or refine your SQL query to see all rows.` (was previously `only the first partition is shown`).
- **`details.executionStats`** retains `partitionCount` for back-compat and adds `partitionsFetched` / `partitionsTotal`.

### Migration notes

- **Existing installs are unaffected on upgrade.** All three new behaviors default to safe values:
  - `bulkLookupEnabled = true`, but only activates when the configured SQL matches the single-`?` predicate pattern; otherwise execution is per-entity exactly as before.
  - `maxPartitions = 1` preserves the prior partition-0-only fetch behavior.
  - `cacheEnabled = true` with a 5-minute TTL — disable by setting `cacheTtlSeconds = 0` or `cacheEnabled = false` if you require strictly fresh reads on every lookup.
- See `README.md` → "Bulk Lookup SQL Pattern", "Multi-Partition Results", and "Result Caching" for worked examples and SQL guidance.

## 1.0.0 — INT-553 Closeout

First production-ready release. This is a baseline rewrite cutting from the legacy `3.x` versioning, and ships ten audit-driven changes from Linear ticket **INT-553**.

### Added

- **Connectivity probe in `validateOptions`.** When an admin saves integration settings, the integration now performs an unauthenticated `GET /api/v2/statements/<sentinel-uuid>` against the configured `baseUrl` to confirm DNS resolves, the host is reachable, and the credential is at least syntactically usable. Auth-type-aware messages distinguish between bad host, bad credential, missing role, and DNS failure (`ENOTFOUND`/`ETIMEDOUT`). Probe is best-effort and never blocks the save on transient errors.
- **Native copy-to-clipboard buttons** for the statement handle and the rendered SQL query. Implemented as a pure Ember action calling `navigator.clipboard.writeText()` with an `execCommand('copy')` fallback for older browsers — no new npm dependency, no `onMessage` round-trip. Visual confirmation: icon swaps to a checkmark for ~1.5s after a successful copy.
- **"SQL Query" collapsible section** in the result overlay. Shows the exact SQL that ran (with bind variables already substituted server-side by Snowflake) so analysts can paste it into Snowsight to reproduce or extend.
- **Structured error classification flags** on every result object: `isAuthError`, `isWarehouseError`, `isTimeout`. Each surfaces a distinct summary pill (`🔒 Auth Failed`, `❄️ Warehouse Suspended`, `⏱ Query Cancelled`, `⚠ Query Error`) and a tailored banner with remediation guidance in the overlay body.
- **PII-safe info-level breadcrumb** logged once per JWT generation: `{ authType, fingerprintShort, expiresInSec }`. Lets operators confirm key-pair auth is working without exposing the full fingerprint, claims, or username.

### Changed

- **Cancel-on-timeout replaces pending-state UX.** Queries that exceed the poll budget (`MAX_POLL_ATTEMPTS` × backoff) are now actively cancelled on the Snowflake server via `POST /api/v2/statements/<handle>/cancel` to immediately free the warehouse. The integration then surfaces a hard `⏱ Query Cancelled` overlay with the statement handle and remediation hints. The previous "Check Query Status" button and the entire `onMessage` handler have been removed — overlays are now always in a terminal state.
- **5xx errors are retried with the same `requestId`.** Snowflake treats requests as idempotent when the `requestId` query parameter is held constant across attempts. The integration now retries up to 3 times on 502/503/504 with `[1s, 2s, 4s]` backoff before failing. This is invisible to the analyst on the happy path and dramatically reduces transient false negatives during Snowflake control-plane blips.
- **Warehouse-suspended detection.** Snowflake error codes `000605` and `000627`, plus regex matches on `"warehouse [\w_]+ is suspended"` and `"warehouse [\w_]+ is currently being resumed"` in the error message, are now classified as `isWarehouseError`. When the warehouse resumes mid-poll, a `❄️ Warehouse Resumed` summary tag is added so analysts know the next lookup will be faster.
- **OAuth 401 affordance.** When `authType=oauth` and the API returns 401, the error message now reads "OAuth token rejected — the token may have expired or been revoked. Refresh the OAuth Token in integration settings." The matching banner in the overlay reinforces the remediation step. Key-pair 401s (mis-configured user/account/private key) get a different message.
- **PII logs demoted to TRACE.** Three call sites that previously logged JWT claims, IDP claims, or the resolved Snowflake username at INFO level are now TRACE. Operators using default `info` log levels will no longer see PII in `info.log`. INFO-level breadcrumbs are now strictly metadata-only.

### Fixed

- **Partition parameter on poll requests.** `pollStatement` now correctly forwards `partition=0` so the first partition's data is returned on completion, instead of relying on Snowflake's default behaviour which has changed across API versions.
- **`User-Agent` header set to `polarity-snowflake-integration/1.0.0`** on every request, making integration traffic visible in Snowflake's `QUERY_HISTORY` view for capacity planning.
- **JWT cache no longer regenerates on every lookup.** Tokens are cached in-process and reused until 5 minutes before expiry, eliminating ~50ms of crypto work per entity lookup.

### Removed

- **`onMessage` handler.** The "Check Query Status" pending-state UX has been replaced by hard cancel-on-timeout. The whole code path (server handler, button, action, computed property) is gone.
- **`details.complete = false` branch.** Result objects always now arrive in a terminal state (success or classified error), simplifying the template.

### Migration notes

- **Version reset from 3.x → 1.0.0.** The legacy `3.x` lineage was internal-only and never published. This is the first stable, externally-supported release.
- **Existing installs:** No config changes required. The new `select`-typed option default shape is backward compatible with the prior single-string defaults via the `getOpt()` normalizer.
- **Query timeout setting:** If you previously increased the timeout to compensate for the pending-state retry UX, you can usually lower it back to the default 30s — the 5xx retry logic absorbs most transient slowness.

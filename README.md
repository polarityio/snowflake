# Snowflake Integration for Polarity

Query Snowflake with an admin-defined SQL template and display results directly in the Polarity overlay. Supports both OAuth and Key-Pair JWT authentication.

## Integration UUID

`2283541c-987e-4996-b6ce-41bde106d8ae`

## Entity Types Supported

IPv4, IPv6, Domain, URL, Email, MD5, SHA1, SHA256, CVE, MAC — plus any admin-configured custom types via `supportsAdditionalCustomTypes`.

## Required Snowflake Scope

The Snowflake user must have:
- `USAGE` on the target warehouse, database, and schema
- `SELECT` on the tables queried
- SQL API access enabled (`ALLOW_CLIENT_MFA_CACHING` not required for service accounts)

## Authentication

### Option A — OAuth Token
1. Obtain a Snowflake OAuth access token (see [Snowflake OAuth docs](https://docs.snowflake.com/en/user-guide/oauth-intro)).
2. Set **Authentication Type** → `OAuth Token`.
3. Paste the token into the **OAuth Token** field.
4. Tokens expire — update the field when the token is refreshed.

### Option B — Key-Pair JWT (Recommended for service accounts)
1. Generate an RSA key pair:
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```
2. Assign the public key to your Snowflake user:
   ```sql
   ALTER USER <username> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without headers>';
   ```
3. In Polarity settings, set **Authentication Type** → `Key-Pair JWT` and fill in:
   - **Username** — your Snowflake service account username
   - **Private Key (PEM)** — full contents of `rsa_key.p8` (including headers)
   - **Private Key Passphrase** — if the key was generated with `-v2 des3`

The integration generates and caches JWTs automatically (refreshed every 54 minutes).

## Configuration Options

| Option | Description |
|--------|-------------|
| Account Identifier | Snowflake account locator, e.g. `xy12345.us-east-1` |
| Authentication Type | `OAuth Token` or `Key-Pair JWT` |
| OAuth Token | Bearer token (OAuth only) |
| Username | Snowflake username (Key-Pair only) |
| Private Key (PEM) | RSA private key PEM (Key-Pair only) |
| Private Key Passphrase | Passphrase if key is encrypted |
| Warehouse | (Optional) Target warehouse |
| Database | (Optional) Target database |
| Schema | (Optional) Target schema |
| Role | (Optional) Role to assume |
| SQL Query Template | SQL with `?` placeholders for entity value |
| Entity Binding Type | Snowflake type for `?` binding — `TEXT`, `FIXED`, `REAL`, `BOOLEAN` |
| Query Timeout (s) | Default: 30. Set 0 for max (604800 s). |
| Result Limit (rows) | Default: 100. Enforced via `rows_per_resultset`. |
| Summary Attributes | Comma-delimited column names for summary tags. Format: `Label:COLUMN` or `COLUMN`. |
| Max Summary Items | Default: 3 |
| Detail Attributes | Comma-delimited column names for detail panel. Blank = all columns. |
| Item Title Attribute | Column name to use as section header per row. |
| Bulk Lookup Enabled | Default: `true`. When the SQL Query Template uses a single `?` in `= ?` or `IN (?)`, multiple entities in a hover are coalesced into one query and split by predicate column. Falls back transparently for queries that don't match the pattern. |
| Max Result Partitions | Default: `1`. Maximum number of Snowflake result partitions to fetch per query (Snowflake splits large result sets into ~10MB partitions). Hard cap: `10`. |
| Result Cache Enabled | Default: `true`. Cache successful lookup results in process memory keyed by SQL + entity + warehouse/role/database/schema. Errors are never cached. |
| Result Cache TTL (s) | Default: `300`. Cache lifetime in seconds. Set to `0` to disable caching while keeping `cacheEnabled` toggled on for ops control. |

## SQL Query Template

Use `?` as a placeholder for the entity value. All `?` placeholders in the query receive the same entity value.

**Examples:**
```sql
-- Single placeholder
SELECT threat_score, category, last_seen FROM security.events WHERE src_ip = ?

-- Multiple placeholders (same value)
SELECT * FROM events WHERE src_ip = ? OR dst_ip = ?

-- CVE lookup
SELECT host, severity, remediation FROM vuln_data WHERE cve_id = ?
```

## Summary and Detail Attribute Format

Comma-delimited column names, with optional label prefix:

```
THREAT_SCORE, CATEGORY, LAST_SEEN           → uses column name as label
Score:THREAT_SCORE, :CATEGORY               → custom label / no label
```

## Async Query Handling

Queries are submitted asynchronously and polled for completion with exponential backoff (`[500ms, 1s, 2s, 3s, 4s]`, then `[6s, 8s, 10s]`). If a query exceeds the poll budget, it is **cancelled on the Snowflake server** to free the warehouse, and the overlay surfaces a `⏱ Query Cancelled` banner with the statement handle and remediation hints.

If you regularly hit the cancel-on-timeout path, raise the **Query Timeout (s)** option, narrow the SQL `WHERE` clause, or lower the **Result Limit (rows)**.

## Bulk Lookup SQL Pattern

When **Bulk Lookup Enabled** is on (the default) and the SQL Query Template matches a specific shape, the integration coalesces multiple entities from a single Polarity hover into **one** Snowflake query, then splits the result rows back to each entity by matching the predicate column case-insensitively.

**Required SQL shape:**

- Exactly **one** `?` placeholder in the query.
- The `?` must appear in either an equality predicate (`column = ?`) or an `IN` predicate (`column IN (?)`).
- The same column referenced in the predicate must also be present in the `SELECT` list (the integration uses it as the row-to-entity matching key).

**Worked example — bulk eligible:**

```sql
-- Original SQL with a single ? in a = predicate
SELECT src_ip, threat_score, category, last_seen
FROM   security.events
WHERE  src_ip = ?
```

For a hover containing 3 IPs, the integration rewrites and submits this:

```sql
SELECT src_ip, threat_score, category, last_seen
FROM   security.events
WHERE  src_ip IN (?, ?, ?)
```

…then partitions the returned rows into per-entity buckets by `src_ip`.

**Falls back to per-entity execution when:**

- The SQL has multiple `?` placeholders (e.g., `WHERE src_ip = ? OR dst_ip = ?`).
- The predicate column isn't a top-level column reference (e.g., `WHERE LOWER(src_ip) = ?`).
- The predicate column doesn't appear in the result set.
- Only one entity is in the hover (no benefit to bulking).

The integration logs an INFO-level line each time it falls back, so you can audit your SQL against the bulk pattern.

## Multi-Partition Results

Snowflake splits large result sets into ~10MB partitions. By default this integration fetches **partition 0 only** (`maxPartitions = 1`) to match prior behavior. Increase **Max Result Partitions** to fetch additional partitions serially via `GET /api/v2/statements/<handle>?partition=N`.

The truncation banner now displays `Showing N of M result partitions` so you can see at a glance whether you've retrieved everything. Hard cap is `10` partitions per query.

When raising this, also consider:

- Raising **Result Limit (rows)** if your SQL applies a `LIMIT` clause itself.
- The Polarity client renders one card per row, so values above ~5 partitions are usually only useful for back-end reducer pipelines (AI assistant, exports), not the overlay UI.
- Per-partition fetch failures are best-effort: any rows successfully fetched are still returned, and a TRACE-level log line records the partition that failed.

## Result Caching

Lookup results are cached in **process memory** keyed by SHA-256 of `(rendered SQL + entity value + warehouse + role + database + schema)`. Default TTL is **300 seconds**.

**Behavior:**

- **Errors are never cached.** Auth failures, warehouse suspended, query cancelled, and any classified error all force a fresh query on the next lookup.
- **Lazy eviction on read** — expired entries are dropped when re-encountered, no background sweeper.
- **Per-process scope** — cache does not persist across integration restarts and is not shared between Polarity workers.
- **Bypass on non-deterministic SQL** — when the SQL contains any of `now()`, `current_timestamp()`, `current_date()`, `current_time()`, `sysdate()`, `getdate()`, `random()`, or `uuid_string()`, the cache is bypassed in both directions: results are neither read from nor written to the cache. This protects analysts from stale time-bound or random-sampled data.

**To disable:** set **Result Cache TTL (s)** to `0` (caching is skipped while preserving the option toggle for ops visibility), or set **Result Cache Enabled** to `false`.

## Resilience Features (v1.0.0)

- **5xx retry with idempotent `requestId`** — the integration retries up to 3 times on `502`/`503`/`504` responses with `[1s, 2s, 4s]` backoff, reusing the same `requestId` so Snowflake treats the request as a single logical operation.
- **Warehouse-suspended detection** — error codes `000605`/`000627` are surfaced with a `❄️ Warehouse Suspended` pill and a banner explaining that Snowflake auto-resumes warehouses on first use. If the warehouse is resumed mid-query, a `❄️ Warehouse Resumed` summary tag is added.
- **OAuth 401 affordance** — when an OAuth token is rejected, the overlay shows a `🔒 Auth Failed` banner with explicit instructions to refresh the OAuth token in integration settings. Key-pair 401s receive a different message indicating a credential or username/account mismatch.
- **Connectivity probe in `validateOptions`** — saving integration settings now triggers a lightweight `GET /api/v2/statements/<sentinel-uuid>` probe to confirm the host is reachable and the credential is at least syntactically usable. DNS, host, and credential errors are surfaced as field-level errors before the configuration is committed.

## Copy-to-Clipboard Affordances

The result overlay exposes copy buttons for:
- The **Statement Handle** in the Query Metadata section — useful for cross-referencing in Snowsight `QUERY_HISTORY`.
- The **Rendered SQL Query** in the SQL Query collapsible section — paste directly into a Snowsight worksheet to reproduce or extend the lookup.

Both buttons use the browser-native Clipboard API with an `execCommand('copy')` fallback. No content leaves the browser.

## PII and Logging

User identifiers (resolved Snowflake username, JWT claims, IDP claims, full key fingerprint) are logged at **TRACE** level only. INFO-level breadcrumbs include only the auth type, a short fingerprint prefix, and the JWT TTL. To debug auth issues, temporarily set `logging.level` to `trace` in `config.json`.

## Changelog

See [CHANGELOG.md](./CHANGELOG.md) for the full release notes (v1.0.0 baseline + v1.1.0 bulk lookup, multi-partition fetch, and result caching).

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

If Snowflake does not return results within ~11 seconds (5 polling attempts), the overlay displays a **"Check Query Status"** button. Click it to re-poll — results will populate when the query completes.

## Changelog

### v1.0.0
- Initial release
- OAuth and Key-Pair JWT authentication
- Async polling with exponential backoff
- Configurable SQL query template with `?` bindings
- Summary / Detail attribute filtering
- Client-side filter and paging (5 rows/page)
- Collapsible metadata section (statement handle, elapsed time, row count)
- Truncation warning when Snowflake returns multiple result partitions

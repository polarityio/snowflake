'use strict';

module.exports = {
  name: 'Snowflake',
  acronym: 'SNOW',
  description:
    'Query Snowflake with an admin-defined SQL template and display results directly in the Polarity overlay.',
  entityTypes: ['IPv4', 'IPv6', 'domain', 'url', 'email', 'MD5', 'SHA1', 'SHA256', 'cve', 'MAC'],
  supportsAdditionalCustomTypes: true,
  defaultColor: 'light-blue',
  styles: ['./styles/styles.less'],
  onDemandOnly: false,
  block: {
    component: { file: './components/block.js' },
    template: { file: './templates/block.hbs' }
  },
  request: {
    cert: '',
    key: '',
    passphrase: '',
    ca: '',
    proxy: '',
    rejectUnauthorized: true
  },
  logging: { level: 'info' },
  options: [
    {
      key: 'accountIdentifier',
      name: 'Account Identifier',
      description:
        'Your Snowflake account locator (e.g., xy12345.us-east-1). Do not include a trailing slash or https://.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'authType',
      name: 'Authentication Type',
      description: 'Select the authentication method to use when connecting to Snowflake.',
      default: { value: 'oauth', display: 'OAuth Token' },
      type: 'select',
      options: [
        { value: 'oauth', display: 'OAuth Token' },
        { value: 'keypair', display: 'Key-Pair JWT' }
      ],
      multiple: false,
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'oauthToken',
      name: 'OAuth Token',
      description:
        'Bearer token for OAuth authentication. Required when Authentication Type is set to "OAuth Token". Tokens expire — refresh as needed.',
      default: '',
      type: 'password',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'username',
      name: 'Username',
      description: 'Snowflake username for Key-Pair JWT authentication. Required when Authentication Type is "Key-Pair JWT".',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'privateKey',
      name: 'Private Key (PEM)',
      description:
        'PEM-encoded RSA private key for Key-Pair JWT authentication. Include the full key including headers. Required when Authentication Type is "Key-Pair JWT".',
      default: '',
      type: 'password',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'privateKeyPassphrase',
      name: 'Private Key Passphrase',
      description:
        'Passphrase for the private key if it is encrypted. Leave blank if the private key is not encrypted.',
      default: '',
      type: 'password',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'warehouse',
      name: 'Warehouse',
      description: '(Optional) Snowflake warehouse to use for query execution. Defaults to the user\'s DEFAULT_WAREHOUSE.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'database',
      name: 'Database',
      description: '(Optional) Target Snowflake database (case-sensitive). Defaults to the user\'s DEFAULT_NAMESPACE.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'schema',
      name: 'Schema',
      description: '(Optional) Target Snowflake schema (case-sensitive). Defaults to the user\'s DEFAULT_NAMESPACE.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'role',
      name: 'Role',
      description: '(Optional) Role to assume for query execution (case-sensitive). Defaults to the user\'s DEFAULT_ROLE.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'query',
      name: 'SQL Query Template',
      description:
        'SQL query to execute for each entity. Use ? as a placeholder for the entity value (all ? placeholders receive the same entity value). Example: SELECT threat_score, category FROM security.events WHERE src_ip = ?',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'bindingType',
      name: 'Entity Binding Type',
      description: 'The Snowflake binding type for the entity value placeholder (?). Use TEXT for most string-based entity types.',
      default: { value: 'TEXT', display: 'TEXT (string)' },
      type: 'select',
      options: [
        { value: 'TEXT', display: 'TEXT (string)' },
        { value: 'FIXED', display: 'FIXED (integer)' },
        { value: 'REAL', display: 'REAL (float)' },
        { value: 'BOOLEAN', display: 'BOOLEAN' }
      ],
      multiple: false,
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'queryTimeout',
      name: 'Query Timeout (seconds)',
      description: 'Maximum execution time in seconds before the query is cancelled. Set to 0 for no limit (maximum 604800 s). Defaults to 30.',
      default: 30,
      type: 'number',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'resultLimit',
      name: 'Result Limit (rows)',
      description: 'Maximum number of rows to return per query. Enforced server-side via rows_per_resultset. Defaults to 100.',
      default: 100,
      type: 'number',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'summaryAttributes',
      name: 'Summary Attributes',
      description:
        'Comma-delimited list of column names to display as tags in the collapsed summary view. Optionally prefix with a label using "label:column" syntax. If blank, a result count badge is shown.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'maxSummaryItems',
      name: 'Max Summary Items',
      description: 'Maximum number of summary tags to show per entity. Defaults to 3.',
      default: 3,
      type: 'number',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'detailAttributes',
      name: 'Detail Attributes',
      description:
        'Comma-delimited list of column names to display in the detail panel. Optionally prefix with a label using "label:column" syntax. If blank, all returned columns are shown.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'itemTitleAttribute',
      name: 'Item Title Attribute',
      description: 'Column name whose value is used as the section title for each result row. If blank, rows are titled "Result 1", "Result 2", etc.',
      default: '',
      type: 'text',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'bulkLookupEnabled',
      name: 'Enable Bulk Lookup',
      description:
        'When enabled and the SQL Query Template uses a single ? placeholder in either an "= ?" or "IN (?)" predicate, multiple entities in the same lookup batch are coalesced into one Snowflake query using "IN (?, ?, ...)" expansion. Result rows are then mapped back to each entity by matching the column referenced in the predicate. If the SQL contains multiple ? placeholders or no recognizable predicate, the integration falls back to per-entity queries automatically. See README for the bulk pattern and worked example. Default: enabled.',
      default: true,
      type: 'boolean',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'maxPartitions',
      name: 'Max Result Partitions',
      description:
        'Maximum number of Snowflake result-set partitions to fetch per query. Snowflake splits large result sets into ~10MB partitions; partition 0 is returned on completion and additional partitions are fetched serially via /api/v2/statements/<handle>?partition=N. Defaults to 1 (only the first partition is fetched, matching legacy behavior). Maximum: 10. When the result has more partitions than the configured cap, the overlay shows a truncation banner with "N of M partitions" and a Logger.warn is emitted.',
      default: 1,
      type: 'number',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'cacheEnabled',
      name: 'Enable Result Cache',
      description:
        'When enabled, formatted lookup results are cached in process memory keyed by SHA-256 of (rendered SQL + entity value + warehouse + role + database + schema). Cache is automatically bypassed when the SQL contains non-deterministic functions (now/current_timestamp/current_date/current_time/sysdate/getdate/random/uuid_string). Cache is per integration instance (shared across all users) and lazily evicted on read. Default: enabled.',
      default: true,
      type: 'boolean',
      userCanEdit: false,
      adminOnly: true
    },
    {
      key: 'cacheTtlSeconds',
      name: 'Cache TTL (seconds)',
      description:
        'Number of seconds a cached lookup result is considered fresh. Once expired, the next lookup for the same entity refetches from Snowflake and overwrites the cache. Set to 0 to effectively disable caching while leaving the cache infrastructure on. Default: 300 (5 minutes).',
      default: 300,
      type: 'number',
      userCanEdit: false,
      adminOnly: true
    }
  ]
};

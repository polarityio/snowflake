'use strict';

const { get } = require('lodash');

/**
 * Parses a Snowflake ResultSet (200 response) into an array of plain row objects.
 *
 * Snowflake returns:
 *   resultSetMetaData.rowType  → [{ name, type, ... }, ...]
 *   data                       → [["val1", "val2"], ...]
 *
 * We map each row array to { COLUMN_NAME: "value" } using the rowType column names.
 * Column names are normalised to UPPERCASE to match Snowflake's convention.
 *
 * Then we apply the detailAttrList filter and itemTitleAttr to shape the display object:
 * {
 *   title: "value of itemTitleAttr column or null",
 *   attributes: [{ key: "Label", value: "val" }, ...],
 *   raw: { COLUMN: value, ... }          ← full row for summary tag access
 * }
 *
 * @param {object} resultSet - Snowflake ResultSet body
 * @param {Array<{label:string, column:string}>} detailAttrList - parsed detail attr config
 * @param {string} itemTitleAttr - uppercase column name to use as row title
 * @returns {Array<object>} mapped display rows
 */
function mapResultRows(resultSet, detailAttrList, itemTitleAttr) {
  const rowType = resultSet?.resultSetMetaData?.rowType || [];
  const data = resultSet?.data || [];

  // Build column name index (UPPERCASE)
  const columnNames = rowType.map((col) => col.name.toUpperCase());

  const rows = data.map((rowArray, rowIndex) => {
    const raw = {};
    columnNames.forEach((colName, i) => {
      raw[colName] = rowArray[i] ?? null;
    });

    // Apply detail attribute filter — if empty, show all columns
    let displayAttributes;
    if (detailAttrList.length > 0) {
      displayAttributes = detailAttrList
        .filter(({ column }) => raw.hasOwnProperty(column))
        .map(({ label, column }) => ({ key: label || column, value: raw[column] }));
    } else {
      displayAttributes = columnNames.map((col) => ({ key: col, value: raw[col] }));
    }

    const title = itemTitleAttr && raw.hasOwnProperty(itemTitleAttr)
      ? String(raw[itemTitleAttr])
      : null;

    return {
      index: rowIndex + 1,
      title,
      attributes: displayAttributes,
      raw, // retained for summary tag resolution
      resultAsString: JSON.stringify(raw).toLowerCase() // for the filter input
    };
  });

  return rows;
}

/**
 * Builds the summary tag array from the first N result rows.
 *
 * summaryAttrList: [{ label, column }, ...]
 *   - If no attrs configured: returns ["N Results"] count badge
 *   - For each configured attr: shows "<label>: value" or just "value" if no label
 *   - Capped at maxSummaryItems total tags
 */
function buildSummaryTags(rows, summaryAttrList, maxSummaryItems) {
  if (rows.length === 0) return [];

  if (summaryAttrList.length === 0) {
    return [`${rows.length} Result${rows.length === 1 ? '' : 's'}`];
  }

  const tags = [];
  for (const row of rows) {
    for (const { label, column } of summaryAttrList) {
      if (tags.length >= maxSummaryItems) break;
      const value = row.raw[column];
      if (value !== null && value !== undefined && value !== '') {
        tags.push(label ? `${label}: ${value}` : String(value));
      }
    }
    if (tags.length >= maxSummaryItems) break;
  }

  if (tags.length === 0) {
    return [`${rows.length} Result${rows.length === 1 ? '' : 's'}`];
  }

  return tags;
}

/**
 * Parses a comma-delimited attribute string into an array of { label, column } objects.
 *
 * Format options:
 *   "COLUMN_NAME"             → { label: "COLUMN_NAME", column: "COLUMN_NAME" }
 *   "My Label:COLUMN_NAME"    → { label: "My Label", column: "COLUMN_NAME" }
 *   ":COLUMN_NAME"            → { label: "", column: "COLUMN_NAME" }  (no label)
 */
function parseAttributeList(attrString) {
  if (!attrString || !attrString.trim()) return [];
  return attrString
    .split(',')
    .map((entry) => {
      const trimmed = entry.trim();
      const colonIndex = trimmed.indexOf(':');
      if (colonIndex === -1) {
        // No colon → use column name as label
        const column = trimmed.toUpperCase();
        return { label: column, column };
      }
      const label = trimmed.slice(0, colonIndex).trim();
      const column = trimmed.slice(colonIndex + 1).trim().toUpperCase();
      return { label, column };
    })
    .filter(({ column }) => column.length > 0);
}

/**
 * Converts any thrown error/object into a readable JSON-safe object for logging.
 */
function parseErrorToReadableJSON(error) {
  return error instanceof Error
    ? {
        message: error.message,
        stack: error.stack,
        status: error.status,
        userMessage: error.userMessage
      }
    : JSON.parse(JSON.stringify(error, Object.getOwnPropertyNames(error)));
}

module.exports = { mapResultRows, buildSummaryTags, parseAttributeList, parseErrorToReadableJSON };

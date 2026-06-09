'use strict';

const { get } = require('lodash');

const TIMESTAMP_TYPES = new Set(['timestamp_ntz', 'timestamp_ltz', 'timestamp_tz', 'timestamp']);
const DATE_TYPES = new Set(['date']);
const TIME_TYPES = new Set(['time']);
const SEMI_STRUCTURED_TYPES = new Set(['variant', 'object', 'array']);

function formatTimestamp(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return rawValue;
  const stripped = String(rawValue).split(' ')[0];
  const secs = parseFloat(stripped);
  if (!isFinite(secs)) return rawValue;
  const ms = Math.round(secs * 1000);
  const d = new Date(ms);
  if (isNaN(d.getTime())) return rawValue;
  const pad = (n) => String(n).padStart(2, '0');
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())} UTC`
  );
}

function formatDate(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return rawValue;
  const days = parseInt(rawValue, 10);
  if (!isFinite(days)) return rawValue;
  const d = new Date(days * 86400000);
  if (isNaN(d.getTime())) return rawValue;
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

function formatTime(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return rawValue;
  const str = String(rawValue);
  const dotIndex = str.indexOf('.');
  if (dotIndex !== -1) return str.slice(0, dotIndex);
  return str;
}

/**
 * Parses a semi-structured value and returns an array of {key, value} sub-attributes,
 * or null if the value is a scalar/unparseable.
 *
 * Objects → [{key: "fieldName", value: "..."}, ...]
 * Arrays  → [{key: "[0]", value: "..."}, ...]
 */
function parseSemiStructured(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === '') return null;
  let parsed;
  try {
    parsed = JSON.parse(rawValue);
  } catch (_) {
    return null; // not valid JSON — fall back to raw string
  }
  if (Array.isArray(parsed)) {
    if (parsed.length === 0) return [{ key: '(empty)', value: '' }];
    return parsed.map((item, i) => ({
      key: `[${i}]`,
      value: typeof item === 'object' && item !== null ? JSON.stringify(item) : String(item ?? 'null')
    }));
  }
  if (typeof parsed === 'object' && parsed !== null) {
    const entries = Object.entries(parsed);
    if (entries.length === 0) return [{ key: '(empty)', value: '' }];
    return entries.map(([k, v]) => ({
      key: k,
      value: typeof v === 'object' && v !== null ? JSON.stringify(v) : String(v ?? 'null')
    }));
  }
  // Scalar (string/number/bool) — just return as string
  return null;
}

/**
 * Returns true if a string value looks like a JSON object or array.
 */
function looksLikeJson(val) {
  if (typeof val !== 'string') return false;
  const t = val.trim();
  return (t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'));
}

/**
 * Builds an HTML string representing a JSON object/array for inline rendering.
 * Produces a table of key → value rows. Falls back to escaped raw string on failure.
 */
function buildJsonHtml(rawValue) {
  const entries = parseSemiStructured(rawValue);
  if (!entries) return null;
  const escHtml = (s) =>
    String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  const rows = entries
    .map(
      ({ key, value }) =>
        `<tr><td class="snow-j-k">${escHtml(key)}</td>` +
        `<td class="snow-j-v">${escHtml(value)}</td></tr>`
    )
    .join('');
  return `<table class="snow-json-tbl">${rows}</table>`;
}

/**
 * Maps a single row's columns into display attribute objects: { key, value }
 * JSON detection and expansion happens client-side in block.js to avoid
 * platform HTML-escaping of string values.
 */
function buildDisplayAttributes(columnNames, columnTypes, rawRow, displayRaw, detailAttrList) {
  const buildAttr = (label, colName) => ({ key: label || colName, value: displayRaw[colName] });

  if (detailAttrList.length > 0) {
    return detailAttrList
      .filter(({ column }) => Object.prototype.hasOwnProperty.call(displayRaw, column))
      .map(({ label, column }) => buildAttr(label, column));
  }
  return columnNames.map((col) => buildAttr(col, col));
}

function formatCellValue(rawValue, colType) {
  if (TIMESTAMP_TYPES.has(colType)) return formatTimestamp(rawValue);
  if (DATE_TYPES.has(colType)) return formatDate(rawValue);
  if (TIME_TYPES.has(colType)) return formatTime(rawValue);
  // Round floats to avoid IEEE 754 noise (e.g. 48.853409999999997 -> 48.85341)
  if (colType === 'real' && rawValue !== null && rawValue !== undefined && rawValue !== '') {
    const n = Number(rawValue);
    if (isFinite(n)) return parseFloat(n.toPrecision(7));
  }
  // Semi-structured types are handled via buildDisplayAttributes, not here
  return rawValue;
}

function mapResultRows(resultSet, detailAttrList, itemTitleAttr) {
  const rowType = resultSet?.resultSetMetaData?.rowType || [];
  const data = resultSet?.data || [];

  const columnNames = rowType.map((col) => col.name.toUpperCase());
  const columnTypes = {};
  rowType.forEach((col) => {
    columnTypes[col.name.toUpperCase()] = (col.type || '').toLowerCase();
  });


  const rows = data.map((rowArray, rowIndex) => {
    const raw = {};
    columnNames.forEach((colName, i) => {
      raw[colName] = rowArray[i] ?? null;
    });

    const displayRaw = {};
    columnNames.forEach((colName) => {
      const colType = columnTypes[colName] || '';
      displayRaw[colName] = formatCellValue(raw[colName], colType);
    });

    const displayAttributes = buildDisplayAttributes(
      columnNames, columnTypes, raw, displayRaw, detailAttrList
    );

    const title = itemTitleAttr && Object.prototype.hasOwnProperty.call(displayRaw, itemTitleAttr)
      ? String(displayRaw[itemTitleAttr])
      : null;

    return {
      index: rowIndex + 1,
      title,
      attributes: displayAttributes,
      raw,
      resultAsString: JSON.stringify(raw).toLowerCase()
    };
  });

  return rows;
}

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

function parseAttributeList(attrString) {
  if (!attrString || !attrString.trim()) return [];
  return attrString
    .split(',')
    .map((entry) => {
      const trimmed = entry.trim();
      const colonIndex = trimmed.indexOf(':');
      if (colonIndex === -1) {
        const column = trimmed.toUpperCase();
        return { label: column, column };
      }
      const label = trimmed.slice(0, colonIndex).trim();
      const column = trimmed.slice(colonIndex + 1).trim().toUpperCase();
      return { label, column };
    })
    .filter(({ column }) => column.length > 0);
}

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

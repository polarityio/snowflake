'use strict';

const crypto = require('crypto');
const jwt = require('jsonwebtoken');

// JWT is valid for 59 minutes; we cache and reuse until 5 minutes before expiry
const JWT_LIFETIME_SECONDS = 3540; // 59 min
const JWT_REFRESH_BUFFER_SECONDS = 300; // refresh 5 min early

/**
 * Normalises a Snowflake account identifier for use in JWT claims:
 * - Uppercase
 * - Periods replaced with hyphens
 * Example: "xy12345.us-east-1" → "XY12345-US-EAST-1"
 */
function normalizeAccountForJwt(accountIdentifier) {
  return accountIdentifier.toUpperCase().replace(/\./g, '-');
}

/**
 * Computes the SHA-256 fingerprint of the public key in DER format,
 * base64-encoded, prefixed with "SHA256:".
 * This matches the RSA_PUBLIC_KEY_FP value shown by DESCRIBE USER in Snowflake.
 */
function computePublicKeyFingerprint(publicKeyDer) {
  const hash = crypto.createHash('sha256').update(publicKeyDer).digest('base64');
  return `SHA256:${hash}`;
}

/**
 * Extracts the public key in DER format from the PEM private key.
 */
function extractPublicKeyDer(privateKeyPem, passphrase) {
  const keyObject = passphrase
    ? crypto.createPrivateKey({ key: privateKeyPem, passphrase })
    : crypto.createPrivateKey(privateKeyPem);
  return crypto.createPublicKey(keyObject).export({ type: 'spki', format: 'der' });
}

/**
 * Generates a Snowflake Key-Pair JWT.
 *
 * @param {object} params
 * @param {string} params.accountIdentifier - e.g. "xy12345.us-east-1"
 * @param {string} params.username
 * @param {string} params.privateKey - PEM-encoded RSA private key
 * @param {string} [params.privateKeyPassphrase] - passphrase if key is encrypted
 * @returns {{ token: string, expiresAt: number }} expiresAt is epoch milliseconds
 */
function generateJwt({ accountIdentifier, username, privateKey, privateKeyPassphrase }) {
  const account = normalizeAccountForJwt(accountIdentifier);
  const user = username.toUpperCase();

  const publicKeyDer = extractPublicKeyDer(privateKey, privateKeyPassphrase || undefined);
  const fingerprint = computePublicKeyFingerprint(publicKeyDer);

  const iat = Math.floor(Date.now() / 1000);
  const exp = iat + JWT_LIFETIME_SECONDS;

  const payload = {
    iss: `${account}.${user}.${fingerprint}`,
    sub: `${account}.${user}`,
    iat,
    exp
  };

  const keyObject = privateKeyPassphrase
    ? crypto.createPrivateKey({ key: privateKey, passphrase: privateKeyPassphrase })
    : crypto.createPrivateKey(privateKey);

  const token = jwt.sign(payload, keyObject, { algorithm: 'RS256' });

  return { token, expiresAt: (exp - JWT_REFRESH_BUFFER_SECONDS) * 1000 };
}

/**
 * Returns true if the cached JWT is expired (or within the refresh buffer).
 */
function isJwtExpired(expiresAt) {
  return Date.now() >= expiresAt;
}

module.exports = { generateJwt, isJwtExpired };

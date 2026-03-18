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
 * Normalises a PEM string for use with Node.js crypto.
 * Polarity may store multiline password fields with literal \n sequences
 * instead of real newline characters. This restores them.
 */
function normalizePem(pem) {
  if (!pem) return '';
  return pem
    .replace(/\\n/g, '\n')  // literal \n → real newline
    .replace(/\\r/g, '')    // strip any \r
    .trim();
}

/**
 * Extracts the public key in DER format from the PEM private key.
 * Tries PKCS#8 (preferred) then PKCS#1 (legacy fallback) to maximise compatibility.
 */
function extractPublicKeyDer(privateKeyPem, passphrase) {
  const normalizedPem = normalizePem(privateKeyPem);
  const pemHeader = normalizedPem.split('\n')[0] || '(empty)';

  const tryLoad = (opts) => crypto.createPrivateKey(opts);

  let keyObject;
  try {
    // Primary: auto-detect (works for PKCS#8 encrypted/unencrypted, PKCS#1 unencrypted)
    keyObject = passphrase
      ? tryLoad({ key: normalizedPem, passphrase, format: 'pem' })
      : tryLoad({ key: normalizedPem, format: 'pem' });
  } catch (primaryErr) {
    // Fallback: explicit PKCS#1 unencrypted (no passphrase) — for BEGIN RSA PRIVATE KEY
    if (!passphrase) {
      try {
        keyObject = tryLoad({ key: normalizedPem, format: 'pem', type: 'pkcs1' });
      } catch (_) {
        // fall through to the descriptive error below
      }
    }
    if (!keyObject) {
      // Build a user-actionable error message based on the PEM header
      const isEncryptedPkcs1 =
        pemHeader.includes('RSA PRIVATE KEY') &&
        normalizedPem.includes('Proc-Type: 4,ENCRYPTED');
      const isLegacyCipher =
        normalizedPem.includes('DEK-Info: DES-EDE3') || normalizedPem.includes('DEK-Info: DES');

      let hint =
        `Private key header: "${pemHeader}". ` +
        'This Node.js version (18+) uses OpenSSL 3.x which dropped legacy cipher support. ';

      if (isEncryptedPkcs1 || isLegacyCipher) {
        hint +=
          'Your key is encrypted with a legacy algorithm (3DES/DES). ' +
          'Regenerate it using AES-256 PKCS#8: ' +
          'openssl pkcs8 -topk8 -v2 aes256 -in your_old_key.pem -out rsa_key.p8';
      } else if (pemHeader.includes('RSA PRIVATE KEY')) {
        hint +=
          'Your PKCS#1 key may need to be converted to PKCS#8: ' +
          'openssl pkcs8 -topk8 -nocrypt -in your_key.pem -out rsa_key.p8';
      } else {
        hint +=
          `Original error: ${primaryErr.message}. ` +
          'Ensure your key is a Snowflake-compatible RSA PKCS#8 key generated with: ' +
          'openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -out rsa_key.p8';
      }

      throw new Error(hint);
    }
  }

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
  const normalizedKey = normalizePem(privateKey);
  const account = normalizeAccountForJwt(accountIdentifier);
  const user = username.toUpperCase();

  const publicKeyDer = extractPublicKeyDer(normalizedKey, privateKeyPassphrase || undefined);
  const fingerprint = computePublicKeyFingerprint(publicKeyDer);

  const iat = Math.floor(Date.now() / 1000);
  const exp = iat + JWT_LIFETIME_SECONDS;

  const payload = {
    iss: `${account}.${user}.${fingerprint}`,
    sub: `${account}.${user}`,
    iat,
    exp
  };

  let signingKey;
  try {
    signingKey = privateKeyPassphrase
      ? crypto.createPrivateKey({ key: normalizedKey, passphrase: privateKeyPassphrase, format: 'pem' })
      : crypto.createPrivateKey({ key: normalizedKey, format: 'pem' });
  } catch (e) {
    if (!privateKeyPassphrase) {
      signingKey = crypto.createPrivateKey({ key: normalizedKey, format: 'pem', type: 'pkcs1' });
    } else {
      throw e;
    }
  }

  const token = jwt.sign(payload, signingKey, { algorithm: 'RS256' });

  return { token, expiresAt: (exp - JWT_REFRESH_BUFFER_SECONDS) * 1000 };
}

/**
 * Returns true if the cached JWT is expired (or within the refresh buffer).
 */
function isJwtExpired(expiresAt) {
  return Date.now() >= expiresAt;
}

module.exports = { generateJwt, isJwtExpired };

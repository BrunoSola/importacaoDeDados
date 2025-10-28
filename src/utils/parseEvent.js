// src/utils/parseEvent.js
// - Converte headers para lowercase
// - Lê body (base64 ou texto)
// - Extrai arquivo de multipart/form-data (campo name="file") quando aplicável
// - Suporta teste com x-filename quando não for multipart
// - Aplica limites de segurança (tamanho da parte) e mensagens de erro padronizadas

const DEFAULT_MAX_PART_BYTES = 5 * 1024 * 1024; // 5 MB por parte (arquivo)

function toLowerHeaders(h = {}) {
  return Object.fromEntries(
    Object.entries(h || {}).map(([k, v]) => [String(k).toLowerCase(), v])
  );
}

function getHeader(headers = {}, name) {
  const lh = toLowerHeaders(headers);
  return lh[name.toLowerCase()];
}

function ensureError(message, statusCode = 400) {
  const e = new Error(message);
  e.statusCode = statusCode;
  return e;
}

/**
 * Lê o body do evento e retorna { contentType, filename, buffer }
 * - Se for multipart/form-data: extrai a parte name="file"
 * - Caso contrário: retorna o body inteiro como arquivo (útil p/ testes com base64 bruto)
 * @param {object} event
 * @param {object} opts { maxPartBytes?: number }
 */
function readBodyBase64(event, opts = {}) {
  const maxPartBytes = opts.maxPartBytes || DEFAULT_MAX_PART_BYTES;

  const headers = toLowerHeaders(event.headers);
  const contentType = headers['content-type'] || '';
  const filenameHeader = headers['x-filename'] || '';

  const raw = event.body || '';
  const isB64 = !!event.isBase64Encoded;

  // Limite global do payload bruto
  const rawSize = Buffer.byteLength(raw || '', isB64 ? 'base64' : 'utf8');
  const MAX_REQ_BYTES = 6 * 1024 * 1024; // 6MB
  if (rawSize > MAX_REQ_BYTES) {
    throw ensureError(`Request maior que o permitido (${rawSize} > ${MAX_REQ_BYTES} bytes).`, 413);
  }

  // 1) multipart/form-data → extrai a parte name="file"
  if (contentType.toLowerCase().startsWith('multipart/form-data')) {
    const tryParse = (buf) => parseMultipart(buf, contentType, { maxPartBytes });
    let rawBuffer;
    try {
      rawBuffer = Buffer.from(raw, 'base64');
    } catch {
      rawBuffer = Buffer.from(raw, 'utf-8');
    }
    let parsed = tryParse(rawBuffer);

    // Fallback: alguns proxies perdem isBase64Encoded
    const looksInvalid = !parsed || !parsed.buffer || parsed.buffer.length === 0;
    if (looksInvalid && !isB64) {
      try {
        rawBuffer = Buffer.from(raw, 'base64');
        parsed = tryParse(rawBuffer);
      } catch { /* ignore */ }
    }

    if (!parsed) {
      const e = new Error('Falha ao ler multipart/form-data: parte "file" não encontrada ou inválida.');
      e.statusCode = 400;
      throw e;
    }

    return {
      contentType: parsed.contentType || 'application/octet-stream',
      filename: parsed.filename || filenameHeader || '',
      buffer: parsed.buffer
    };
  }

  // 2) application/json → espera { base64, filename?, contentType? }
  if (contentType.toLowerCase().includes('application/json')) {
    let json;
    try {
      json = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch {
      const e = new Error('JSON inválido no corpo da requisição.');
      e.statusCode = 400;
      throw e;
    }

    // Suporta base64 em "base64" ou "fileBase64"
    let b64 = json.base64 || json.fileBase64 || '';
    if (typeof b64 !== 'string' || !b64) {
      const e = new Error('Campo "base64" (ou "fileBase64") ausente no JSON.');
      e.statusCode = 400;
      throw e;
    }

    // Remove prefixos dataURL, se houver
    b64 = b64.replace(/^data:.*;base64,/, '');

    let buffer;
    try {
      buffer = Buffer.from(b64, 'base64');
    } catch {
      const e = new Error('Base64 inválido.');
      e.statusCode = 400;
      throw e;
    }

    return {
      contentType: json.contentType || contentType || 'application/octet-stream',
      filename: json.filename || filenameHeader || '',
      buffer
    };
  }

  // 3) Outros content-types: base64 puro ou texto
  const rawBuffer = isB64 ? Buffer.from(raw, 'base64') : Buffer.from(raw, 'utf-8');
  return {
    contentType,
    filename: filenameHeader || '',
    buffer: rawBuffer
  };
}



/**
 * Parser simples e robusto para multipart/form-data.
 * Procura especificamente a parte name="file".
 * Retorna { filename, contentType, buffer } ou null.
 * - Tolerante a boundary com aspas
 * - Suporta CRLF e LF
 * - Aplica limite de tamanho da parte (maxPartBytes)
 */
function parseMultipart(buffer, contentTypeHeader, { maxPartBytes = DEFAULT_MAX_PART_BYTES } = {}) {
  if (!buffer || !buffer.length) return null;

  const match = /boundary=([^;]+)/i.exec(contentTypeHeader || '');
  if (!match) return null;

  // remove aspas do boundary se vier como boundary="----XYZ"
  let boundaryVal = match[1].trim();
  if (
    (boundaryVal.startsWith('"') && boundaryVal.endsWith('"')) ||
    (boundaryVal.startsWith("'") && boundaryVal.endsWith("'"))
  ) {
    boundaryVal = boundaryVal.slice(1, -1);
  }

  const boundary = Buffer.from(`--${boundaryVal}`);
  const closingBoundary = Buffer.from(`--${boundaryVal}--`);
  const CRLF = Buffer.from('\r\n');
  const LF = Buffer.from('\n');
  const HEADER_BODY_SEP_CRLF = Buffer.from('\r\n\r\n');
  const HEADER_BODY_SEP_LF = Buffer.from('\n\n');

  // 1) Particiona por boundary
  const parts = splitByBoundary(buffer, boundary, closingBoundary);

  for (const part of parts) {
    if (!part || part.length === 0) continue;

    // 2) Separa headers e body
    let sepIndex = indexOfSubBuffer(part, HEADER_BODY_SEP_CRLF);
    let sepLen = HEADER_BODY_SEP_CRLF.length;
    if (sepIndex === -1) {
      sepIndex = indexOfSubBuffer(part, HEADER_BODY_SEP_LF);
      sepLen = HEADER_BODY_SEP_LF.length;
    }
    if (sepIndex === -1) continue;

    const headersBuf = part.slice(0, sepIndex);
    let body = part.slice(sepIndex + sepLen);

    // remove CRLF ou LF final, se houver
    if (endsWithSubBuffer(body, CRLF)) {
      body = body.slice(0, body.length - CRLF.length);
    } else if (endsWithSubBuffer(body, LF)) {
      body = body.slice(0, body.length - LF.length);
    }

    // valida tamanho da parte
    if (body.length > maxPartBytes) {
      throw ensureError(`Arquivo excedeu o limite (${body.length} > ${maxPartBytes} bytes).`, 413);
    }

    const headersText = headersBuf.toString('utf-8');
    const cdLine = matchHeader(headersText, /^content-disposition:[\s\S]*$/im);
    if (!cdLine) continue;

    const nameMatch = /name="([^"]+)"/i.exec(cdLine);
    if (!nameMatch || nameMatch[1] !== 'file') continue;

    const fileMatch = /filename="([^"]*)"/i.exec(cdLine);
    const filename = fileMatch ? fileMatch[1] : '';

    const ct = matchHeader(headersText, /^content-type:\s*([^\r\n]+)/im);
    const partContentType = ct ? ct.replace(/^content-type:\s*/i, '').trim() : '';

    if (!body || body.length === 0) continue;

    return { filename, contentType: partContentType, buffer: body };
  }

  return null;
}

/**
 * Divide o buffer por boundary, tolerando o fechamento com --boundary-- e variações de quebra de linha.
 */
function splitByBoundary(buf, boundary, closingBoundary) {
  const out = [];
  let start = 0;

  while (start < buf.length) {
    const idx = indexOfSubBuffer(buf, boundary, start);
    const idxClose = indexOfSubBuffer(buf, closingBoundary, start);

    if (idx === -1 && idxClose === -1) break;

    const next = (idx !== -1 && (idxClose === -1 || idx < idxClose)) ? idx : idxClose;
    if (next > start) {
      let chunk = buf.slice(start, next);

      // remove CRLF/LF inicial
      if (chunk.length >= 2 && chunk[0] === 13 && chunk[1] === 10) { // \r\n
        chunk = chunk.slice(2);
      } else if (chunk.length >= 1 && chunk[0] === 10) { // \n
        chunk = chunk.slice(1);
      }

      // remove CRLF/LF final
      if (chunk.length >= 2 && chunk[chunk.length - 2] === 13 && chunk[chunk.length - 1] === 10) {
        chunk = chunk.slice(0, chunk.length - 2);
      } else if (chunk.length >= 1 && chunk[chunk.length - 1] === 10) {
        chunk = chunk.slice(0, chunk.length - 1);
      }

      if (chunk.length) out.push(chunk);
    }

    // move após boundary ou closingBoundary
    const bLen = (next === idx) ? boundary.length : closingBoundary.length;
    start = next + bLen;
  }

  // Parte residual
  if (start < buf.length) {
    let tail = buf.slice(start);
    if (tail.length >= 2 && tail[0] === 13 && tail[1] === 10) tail = tail.slice(2);
    if (tail.length) out.push(tail);
  }

  return out;
}

// Utilitários de buffer
function indexOfSubBuffer(buf, sub, from = 0) {
  if (!sub || !sub.length) return -1;
  for (let i = from; i <= buf.length - sub.length; i++) {
    let ok = true;
    for (let j = 0; j < sub.length; j++) {
      if (buf[i + j] !== sub[j]) { ok = false; break; }
    }
    if (ok) return i;
  }
  return -1;
}

function endsWithSubBuffer(buf, sub) {
  if (!buf || !sub) return false;
  if (sub.length > buf.length) return false;
  for (let i = 0; i < sub.length; i++) {
    if (buf[buf.length - sub.length + i] !== sub[i]) return false;
  }
  return true;
}

function matchHeader(headersText, regex) {
  const m = headersText.match(regex);
  return m ? m[0] : '';
}

function parseFlags(headers) {
  const h = toLowerHeaders(headers);
  const dryRun = String(h['x-dry-run'] || '').toLowerCase() === 'true';
  const preview = String(h['x-preview'] || '').toLowerCase() === 'true';
  return { dryRun, preview };
}

module.exports = { toLowerHeaders, getHeader, readBodyBase64, parseFlags };

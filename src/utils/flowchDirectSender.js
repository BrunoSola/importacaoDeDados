// src/utils/flowchDirectSender.js
const { httpJson } = require('../core/httpClient');

const LOG_RESP = process.env.LOG_RESP === '1';
const LOG_RESP_MAX = Number(process.env.LOG_RESP_MAX || 6);
let __logCount = 0;

function logResp(label, batchIndex, resp) {
  if (!LOG_RESP || __logCount >= LOG_RESP_MAX) return;
  __logCount++;
  const status = resp?.statusCode ?? resp?.status ?? resp?.code ?? null;
  const headers = resp?.headers || {};
  const ct = headers['content-type'] || headers['Content-Type'] || '';
  const raw = resp?.body;
  const bodyLen = typeof raw === 'string' ? raw.length : Buffer.isBuffer(raw) ? raw.length : JSON.stringify(raw || '').length;
  const preview = typeof raw === 'string'
    ? raw.slice(0, 800)
    : Buffer.isBuffer(raw)
      ? raw.toString('utf8', 0, 800)
      : JSON.stringify(raw || '').slice(0, 800);
  console.log('[HTTP-RESP]', { label, batchIndex, status, contentType: ct, bodyLen, bodyPreview: preview });
}

function dividirEmLotes(arr, tamanho) {
  const out = [];
  const step = Math.max(1, tamanho);
  for (let i = 0; i < arr.length; i += step) out.push(arr.slice(i, i + step));
  return out;
}

async function enviarLote({ endpointUrl, method, headers, body, timeoutMs }) {
  try {
    const bodyStr = JSON.stringify(body);
    return await httpJson(endpointUrl, method, headers, bodyStr, timeoutMs);
  } catch (erro) {
    return { statusCode: 599, headers: {}, body: JSON.stringify({ error: erro.message || 'network error' }) };
  }
}

function tryParseJson(raw) {
  if (typeof raw !== 'string') return raw;
  const t = raw.trim();
  if (!t) return raw;
  const c = t[0];
  if (c !== '{' && c !== '[') return raw;
  try { return JSON.parse(t); } catch { return raw; }
}

function normStatus(resp) {
  return Number(resp?.statusCode ?? resp?.status ?? resp?.code ?? 0);
}

async function sendBatchesDirectToFlowch({
  endpointUrl,
  token,
  records,
  batchSize = 100,
  timeoutMs = 15000,
  method = 'POST',
}) {
  const todos = Array.isArray(records) ? records : [records];
  const headers = { Authorization: `integration ${token}`, 'Content-Type': 'application/json', Accept: 'application/json' };

  // FAST-PATH
  if (todos.length <= batchSize) {
    const t0 = Date.now();
    const resp = await enviarLote({ endpointUrl, method, headers, body: todos, timeoutMs });
    logResp('fast-path', 1, resp);
    const duration = Date.now() - t0;
    const code = normStatus(resp);
    const body = tryParseJson(resp.body);
    return {
      endpointUrl,
      batchSize,
      totalBatches: 1,
      totalRecords: todos.length,
      results: [{
        batchIndex: 1,
        size: todos.length,
        statusCode: code,
        durationMs: duration,
        body,
      }],
    };
  }

  // MULTI-LOTES
  const lotes = dividirEmLotes(todos, batchSize);
  const resultados = [];

  for (let i = 0; i < lotes.length; i++) {
    const payload = lotes[i];
    const t0 = Date.now();
    const resp = await enviarLote({
      endpointUrl,
      method,
      headers,
      body: payload,             // <<< FIX: era "payload"
      timeoutMs,
    });
    logResp('batch', i + 1, resp);
    const duration = Date.now() - t0;
    const code = normStatus(resp);
    const parsed = tryParseJson(resp.body);
    resultados.push({
      batchIndex: i + 1,
      size: payload.length,
      statusCode: code,
      durationMs: duration,
      body: parsed,
    });
  }

  return { endpointUrl, batchSize, totalBatches: resultados.length, totalRecords: todos.length, results: resultados };
}

module.exports = { sendBatchesDirectToFlowch };

// src/utils/flowchDirectSender.js
const https = require('https');

function httpJson(urlStr, method, headers, payload, timeoutMs = 15000) {
  return new Promise((resolve) => {
    const url = new URL(urlStr);
    const body = payload == null
      ? ''
      : (typeof payload === 'string' ? payload : JSON.stringify(payload));

    const options = {
      hostname: url.hostname,
      path: url.pathname + (url.search || ''),
      method,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        ...headers,
      },
      timeout: timeoutMs,
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: data }));
    });

    req.on('timeout', () => req.destroy(Object.assign(new Error('ETIMEDOUT'), { code: 'ETIMEDOUT' })));
    req.on('error', (err) => {
      resolve({ statusCode: 599, headers: {}, body: JSON.stringify({ error: err.message || 'network error' }) });
    });

    req.write(body);
    req.end();
  });
}

function chunkArray(arr, size) {
  const out = [];
  const n = Math.max(1, size);
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

/**
 * Envia registros diretamente para o endpoint completo do Flowch em lotes.
 * @param {Object} params
 * @param {string} params.endpointUrl  URL completa do Flowch (ex.: https://int01.flowch.com/integrator/<uuid>/avaliacaoDesempenho)
 * @param {string} params.token        Token cru (sem prefixo); será enviado como "integration <token>"
 * @param {Array|Object} params.records  Array de objetos (ou único objeto)
 * @param {number} [params.batchSize=100] Tamanho do lote
 * @param {number} [params.timeoutMs=15000]
 * @param {string} [params.method='POST']
 */
async function sendBatchesDirectToFlowch({
  endpointUrl,
  token,
  records,
  batchSize = 100,
  timeoutMs = 15000,
  method = 'POST',
}) {
  const all = Array.isArray(records) ? records : [records];
  const batches = chunkArray(all, batchSize);
  const headers = { Authorization: `integration ${token}` };

  const results = [];
  
  for (let i = 0; i < batches.length; i++) {
    const payload = batches[i];
    const t0 = Date.now();
    const resp = await httpJson(endpointUrl, method, headers, payload, timeoutMs);
    const dt = Date.now() - t0;

    let parsed;
    try { parsed = JSON.parse(resp.body); } catch { parsed = resp.body; }

    results.push({
      batchIndex: i + 1,
      size: payload.length,
      statusCode: resp.statusCode,
      durationMs: dt,
      body: parsed,
    });
  }

  return {
    endpointUrl,
    batchSize,
    totalBatches: results.length,
    totalRecords: all.length,
    results,
  };
}

module.exports = { sendBatchesDirectToFlowch };

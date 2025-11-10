// src/utils/flowchDirectSender.js
const { httpJson } = require('../core/httpClient');

function dividirEmLotes(arr, tamanho) {
  const resultado = [];
  const limite = Math.max(1, tamanho);
  for (let i = 0; i < arr.length; i += limite) resultado.push(arr.slice(i, i + limite));
  return resultado;
}

async function enviarLote({ endpointUrl, method, headers, body, timeoutMs }) {
  try {
    const bodyStr = JSON.stringify(body);
    return await httpJson(endpointUrl, method, headers, bodyStr, timeoutMs);
  } catch (erro) {
    return {
      statusCode: 599,
      headers: {},
      body: JSON.stringify({ error: erro.message || 'network error' }),
    };
  }
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

  // FAST-PATH: cabe em 1 chamada ⇒ sem criar cópias/lotes
  if (todos.length <= batchSize){
    const t0 = Date.now();
    const resp = await enviarLote({
      endpointUrl,
      method: method,
      headers: headers,
      body: todos,
      timeoutMs
    })
    const duration = Date.now() - t0;
    let body;
    try {
      const ct = String(resp.headers?.['content-type'] || '');
      body = ct.includes('json') ? JSON.parse(resp.body || '{}') : resp.body;
    } catch {
      body = resp.body;
    }
    return {
      endpointUrl,
      batchSize,
      totalBatches: 1,
      totalRecords: todos.length,
      results: [{
        batchIndex: 1,
        size: todos.length,
        statusCode: resp.statusCode,
        durationMs: duration,
        body,
      }],
    };    
  }

  // Múltiplos lotes (sequencial, preserva comportamento)
  const lotes = dividirEmLotes(todos, batchSize);
  const resultados = [];

  for (let i = 0; i < lotes.length; i++) {
    const body = lotes[i];
    const t0 = Date.now();
    const resp = await enviarLote({
      endpointUrl,
      method: method,
      headers,
      body,
      timeoutMs,
    });
    const duration = Date.now() - t0;

    let bodyStr;
    try { 
      const ct = String(resp.headers?.['content-type'] || '');
      bodyStr = ct.includes('json') ? JSON.parse(resp.body || '{}') : resp.body;
    } catch { 
      bodyStr = resp.body;  
    }
    resultados.push({
      batchIndex: i + 1,
      size: body.length,
      statusCode: resp.statusCode,
      durationMs: duration,
      body: bodyStr,
    });    
  }

  return {
    endpointUrl,
    batchSize,
    totalBatches: resultados.length,
    totalRecords: todos.length,
    results: resultados,
  };
}

module.exports = { sendBatchesDirectToFlowch };

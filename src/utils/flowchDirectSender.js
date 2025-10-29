// src/utils/flowchDirectSender.js
const { httpJson } = require('../core/httpClient');

function dividirEmLotes(arr, tamanho) {
  const resultado = [];
  const limite = Math.max(1, tamanho);
  for (let i = 0; i < arr.length; i += limite) resultado.push(arr.slice(i, i + limite));
  return resultado;
}

async function enviarLote({ endpointUrl, metodo, cabecalhos, carga, timeoutMs }) {
  try {
    return await httpJson(endpointUrl, metodo, cabecalhos, JSON.stringify(carga), timeoutMs);
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
  const lotes = dividirEmLotes(todos, batchSize);
  const cabecalhos = { Authorization: `integration ${token}`, 'Content-Type': 'application/json' };

  const resultados = [];

  for (let i = 0; i < lotes.length; i++) {
    const carga = lotes[i];
    const inicio = Date.now();
    const resposta = await enviarLote({
      endpointUrl,
      metodo: method,
      cabecalhos,
      carga,
      timeoutMs,
    });
    const duracao = Date.now() - inicio;

    let corpoProcessado;
    try { corpoProcessado = JSON.parse(resposta.body); } catch { corpoProcessado = resposta.body; }

    resultados.push({
      batchIndex: i + 1,
      size: carga.length,
      statusCode: resposta.statusCode,
      durationMs: duracao,
      body: corpoProcessado,
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

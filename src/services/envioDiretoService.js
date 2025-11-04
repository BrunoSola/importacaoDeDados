const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');

const LAMBDA_TIMEOUT_MS = 25000;

function validarBatchSize(valor, padrao) {
  return Number.isFinite(valor) && valor > 0 ? valor : padrao;
}

async function executarEnvioDireto({
  registros,
  offsetInicial,
  endpointUrl,
  token,
  batchSize,
  timeoutMs,
  iniciouEm,
  previewAtivo,
  uploadId,
  fileHash,
  margemSegurancaMs,
  totalLinhas,
  dryRun,
  contextoLambda: context,
  apigwSoftTimeoutMs,
}) {
  const tamanhoLote = validarBatchSize(batchSize, 20);

  if (dryRun) {
    const duracao = Date.now() - iniciouEm;
    return respostaJson(200, {
      nextOffset: offsetInicial,
      done: true,
      summary: {
        modo: 'direto-flowch',
        endpointUrl,
        linhasLidas: totalLinhas,
        enviadasAprox: 0,
        errosBatches: 0,
        duracaoMs: duracao,
        dryRun: true,
        preview: !!previewAtivo,
        batchSize: tamanhoLote,
        totalBatches: 0,
        uploadId,
        fileHash,
        size: 0,
        recordsInserted: 0,
        recordsUpdated: 0,
        recordsDeleted: 0,
      },
    });
  }

  const registrosProcessados = registros;
  let indice = Math.min(offsetInicial, registrosProcessados.length);
  const resultados = [];
  const totais = { size: 0, recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };

  while (indice < registrosProcessados.length) {

    const gwLimitMs = Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000;
    const elapsed = Date.now() - iniciouEm;
  
    const budgetAntesDoLote = gwLimitMs - margemSegurancaMs - elapsed;
    if (budgetAntesDoLote <= 0) {
      console.log(`[envioDireto] PRE-GUARD: sem orçamento (gwElapsed=${elapsed}/${gwLimitMs} | margem=${margemSegurancaMs}) -> 206 nextOffset=${indice}`);
      const duracao = Date.now() - iniciouEm;
      const aceitos = resultados.reduce((t, r) => (r.statusCode >= 200 && r.statusCode < 300 ? t + (r.size || 0) : t), 0);
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: indice,
          done: false,
          summary: {
            modo: 'direto-flowch',
            endpointUrl,
            linhasLidas: totalLinhas,
            enviadasAprox: aceitos,
            errosBatches: resultados.filter(r => !(r.statusCode >= 200 && r.statusCode < 300)).length,
            duracaoMs: duracao,
            dryRun: false,
            preview: !!previewAtivo,
            batchSize: tamanhoLote,
            totalBatches: resultados.length,
            uploadId,
            fileHash,
            size: totais.size,
            recordsInserted: totais.recordsInserted,
            recordsUpdated: totais.recordsUpdated,
            recordsDeleted: totais.recordsDeleted,
          },
        }),
      };    
    }

    const fim = Math.min(indice + tamanhoLote, registrosProcessados.length);
    const fatia = registrosProcessados.slice(indice, fim);

      // ----- TIMEOUT EFETIVO DO POST: CAPADO pelo orçamento do gateway -----
    const timeoutBase = Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS;
    const budgetParaRequisicao = Math.max(1000, budgetAntesDoLote - 300); // 300ms folga
    const effectiveTimeoutMs = Math.min(timeoutBase, budgetParaRequisicao);

  console.log(`[envioDireto] efetivoTimeout=${effectiveTimeoutMs}ms | budgetAntes=${budgetAntesDoLote}ms | elapsed=${elapsed}ms | lote=${fatia.length}`);


    const agregado = await sendBatchesDirectToFlowch({
      endpointUrl,
      token,
      records: fatia,
      batchSize: tamanhoLote,
      timeoutMs: effectiveTimeoutMs,
      method: 'POST',
    });

    resultados.push(...agregado.results);
    for (const resultado of agregado.results) {
      const corpo = resultado.body || {};
      totais.size += resultado.size || 0;
      totais.recordsInserted += corpo.recordsInserted || 0;
      totais.recordsUpdated += corpo.recordsUpdated || 0;
      totais.recordsDeleted += corpo.recordsDeleted || 0;
    }

    indice = fim;

    const lambdaRemaining = (context && typeof context.getRemainingTimeInMillis === 'function')
      ? context.getRemainingTimeInMillis()
      : ((Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS) - elapsed);

    const gwElapsed = Date.now() - iniciouEm;
    const deveEncerrar = (lambdaRemaining <= margemSegurancaMs) || (gwElapsed >= (gwLimitMs - margemSegurancaMs));

    console.log(`[envioDireto] remainingMs=${lambdaRemaining}ms | gwElapsed=${elapsed}/${gwLimitMs}ms | margem=${margemSegurancaMs}ms | indice=${indice}/${registrosProcessados.length}`);

    if(deveEncerrar){
    const duracao = Date.now() - iniciouEm;
    const aceitos = resultados.reduce((total, atual) => (
      atual.statusCode >= 200 && atual.statusCode < 300
        ? total + atual.size
        : total
    ), 0);

      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: indice,
          done: false,
          summary: {
            modo: 'direto-flowch',
            endpointUrl,
            linhasLidas: totalLinhas,
            enviadasAprox: aceitos,
            errosBatches: resultados.filter(r => !(r.statusCode >= 200 && r.statusCode < 300)).length,
            duracaoMs: duracao,
            dryRun: false,
            preview: !!previewAtivo,
            batchSize: tamanhoLote,
            totalBatches: resultados.length,
            uploadId,
            fileHash,
            size: totais.size,
            recordsInserted: totais.recordsInserted,
            recordsUpdated: totais.recordsUpdated,
            recordsDeleted: totais.recordsDeleted,
          },
        }),
      };
    }
  }

  const duracao = Date.now() - iniciouEm;
  const aceitos = resultados.reduce((total, atual) => (
    atual.statusCode >= 200 && atual.statusCode < 300
      ? total + atual.size
      : total
  ), 0);

  const amostrasErro = [];
  for (const resultado of resultados) {
    if (!(resultado.statusCode >= 200 && resultado.statusCode < 300) && amostrasErro.length < 5) {
      const corpoTexto = typeof resultado.body === 'string' ? resultado.body : JSON.stringify(resultado.body);
      amostrasErro.push({
        statusCode: resultado.statusCode,
        snippet: String(corpoTexto).slice(0, 400),
      });
    }
  }

  return respostaJson(200, {
    nextOffset: null,
    done: true,
    summary: {
      modo: 'direto-flowch',
      endpointUrl,
      linhasLidas: totalLinhas,
      enviadasAprox: aceitos,
      errosBatches: resultados.filter(r => !(r.statusCode >= 200 && r.statusCode < 300)).length,
      duracaoMs: duracao,
      dryRun: false,
      preview: !!previewAtivo,
      batchSize: tamanhoLote,
      totalBatches: resultados.length,
      uploadId,
      fileHash,
      size: totais.size,
      recordsInserted: totais.recordsInserted,
      recordsUpdated: totais.recordsUpdated,
      recordsDeleted: totais.recordsDeleted,
      errorSamples: amostrasErro,
    },
  });
}

module.exports = {
  executarEnvioDireto,
};

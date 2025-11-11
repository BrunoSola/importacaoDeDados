// src/services/envioDiretoService.js
const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');
const LAMBDA_TIMEOUT_MS = 25000;

// Helpers de status/erros e modos
const getStatus = (r) => Number(r?.statusCode ?? r?.status ?? r?.code ?? 0);
const isOk = (r) => { const sc = getStatus(r); return sc >= 200 && sc < 300; };

// ACCEPT_MODE: 'optimistic' (padrão) ou 'strict'
const ACCEPT_MODE = String(process.env.ACCEPT_MODE || 'optimistic').toLowerCase();

// SINGLE-BATCH: '1' força enviar exatamente 1 lote por invocação (padrão agora)
const SINGLE_BATCH_MODE = String(process.env.SINGLE_BATCH_MODE ?? '1') === '1';

function countErrors(arr) {
  if (ACCEPT_MODE === 'optimistic') {
    // conta só falhas de transporte (sem status ou 599)
    return arr.filter(r => {
      const sc = getStatus(r);
      return !sc || sc === 599;
    }).length;
  }
  // strict: qualquer não-2xx
  return arr.filter(r => !isOk(r)).length;
}

function buildSummary({ endpointUrl, totalLinhas, previewAtivo, tamanhoLote, resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais }) {
  const duracao = Date.now() - iniciouEm;
  // Fonte única da verdade: o que a API confirmou
  const enviadosConfirmados = Math.max(0, indiceAceito - offsetInicial);

  return {
    modo: 'direto-flowch',
    endpointUrl,
    linhasLidas: totalLinhas,
    enviadasAprox: enviadosConfirmados,
    errosBatches: countErrors(resultados),
    duracaoMs: duracao,
    dryRun: false,
    preview: !!previewAtivo,
    batchSize: tamanhoLote,
    totalBatches: resultados.length,
    uploadId,
    fileHash,
    size: enviadosConfirmados,
    recordsInserted: totais.recordsInserted,
    recordsUpdated: totais.recordsUpdated,
    recordsDeleted: totais.recordsDeleted,
  };
}

function validarBatchSize(valor, padrao) { return Number.isFinite(valor) && valor > 0 ? valor : padrao; }

async function executarEnvioDireto({
  registros, offsetInicial, endpointUrl, token, batchSize, timeoutMs, iniciouEm,
  previewAtivo, uploadId, fileHash, margemSegurancaMs, totalLinhas, dryRun,
  contextoLambda: context, apigwSoftTimeoutMs,
  // (opcionais) ajustes do preditor via handler/env
  etaAlpha, etaMultiplier, etaMinMs,
}) {
  const tamanhoLote = validarBatchSize(batchSize, 20);

  // Preditor (EMA) – (mantido como está)
  const alpha = Math.max(0.05, Math.min(0.95, Number(process.env.ETA_ALPHA ?? etaAlpha ?? 0.4)));
  const mult = Math.max(1, Number(process.env.ETA_MULTIPLIER ?? etaMultiplier ?? 1.1));
  const etaMin = Math.max(100, Number(process.env.ETA_MIN_MS ?? etaMinMs ?? 300));
  let etaAvgMs = 0, etaLastMs = 0;
  const etaEstimate = () => Math.max(etaMin, (etaAvgMs || etaLastMs || etaMin)) * mult;
  const etaUpdate = (ms) => { etaLastMs = ms; etaAvgMs = etaAvgMs ? (alpha * ms + (1 - alpha) * etaAvgMs) : ms; };

  // DRY-RUN (mantido)
  if (dryRun) {
    const duracao = Date.now() - iniciouEm;
    return respostaJson(200, {
      nextOffset: offsetInicial, done: true,
      summary: {
        modo: 'direto-flowch', endpointUrl,
        linhasLidas: totalLinhas, enviadasAprox: 0, errosBatches: 0,
        duracaoMs: duracao, dryRun: true, preview: !!previewAtivo,
        batchSize: tamanhoLote, totalBatches: 0, uploadId, fileHash,
        size: 0, recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0,
      },
    });
  }

  const registrosProcessados = registros || [];
  let indice = Math.min(offsetInicial, registrosProcessados.length);

  // Acumuladores
  const resultados = [];
  const totais = { recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };
  let aceitosTotais = 0;           // soma confiável (server-confirmed)
  let indiceAceito = offsetInicial; // nextOffset base

  while (indice < registrosProcessados.length) {
    // Orçamento do API GW (mantido)
    const gwLimitMs = Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000;
    const elapsed = Date.now() - iniciouEm;
    const budgetAntesDoLote = gwLimitMs - margemSegurancaMs - elapsed;

    // Se já temos uma amostra, decide se cabe mais um lote
    if (aceitosTotais > 0 && budgetAntesDoLote <= etaEstimate()) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary: buildSummary({ endpointUrl, totalLinhas, previewAtivo, tamanhoLote, resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais }) }),
      };
    }
    if (budgetAntesDoLote <= 0) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary: buildSummary({ endpointUrl, totalLinhas, previewAtivo, tamanhoLote, resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais }) }),
      };
    }

    const fim = Math.min(indice + tamanhoLote, registrosProcessados.length);
    const fatia = registrosProcessados.slice(indice, fim);
    if (fatia.length === 0) break;

    const timeoutBase = Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS;
    const overheadMs = Math.max(50, Number(process.env.REQ_OVERHEAD_MS || 200));
    const budgetParaRequisicao = Math.max(1000, budgetAntesDoLote - overheadMs);
    const effectiveTimeoutMs = Math.min(timeoutBase, budgetParaRequisicao);

    const t0 = Date.now();
    const agregado = await sendBatchesDirectToFlowch({
      endpointUrl, token, records: fatia, batchSize: tamanhoLote, timeoutMs: effectiveTimeoutMs, method: 'POST',
    });
    etaUpdate(Date.now() - t0);
    resultados.push(...agregado.results);

    // === SOMA PELO QUE O SERVIDOR DISSE ===
    let aceitosNoCiclo = 0;
    for (const resultado of agregado.results) {
      // body já vem parseado pelo sender; não parsear de novo
      const body = resultado.body || {};
      // preferir `resultado.inserted` que o sender já normaliza
      const inserted = Number.isFinite(Number(resultado.inserted)) ? Number(resultado.inserted) : Number(body.recordsInserted || 0);
      const updated  = Number(body.recordsUpdated || 0);
      const deleted  = Number(body.recordsDeleted || 0);

      // fallback extra: se a API de inclusão devolveu array "records" com IDs
      const recIdsCount = Array.isArray(body.records) ? body.records.length : 0;

      const byCounters = (inserted || recIdsCount) + updated + deleted;

      if (byCounters > 0) {
        aceitosNoCiclo += byCounters;
        totais.recordsInserted += (inserted || recIdsCount);
        totais.recordsUpdated  += updated;
        totais.recordsDeleted  += deleted;
      } else {
        // último fallback: tamanho do lote (compatibilidade)
        aceitosNoCiclo += Number(resultado.size || 0);
        totais.recordsInserted += Number(resultado.size || 0);
      }
    }

    aceitosTotais += aceitosNoCiclo;
    indiceAceito = offsetInicial + aceitosTotais; // próximo cursor = base + confirmados

    // move o cursor local (independente do aceito)
    indice = fim;

    // SINGLE-BATCH: encerra após 1 lote
    if (String(process.env.SINGLE_BATCH_MODE ?? '1') === '1') {
      const temMais = fim < registrosProcessados.length;
      const summary = buildSummary({ endpointUrl, totalLinhas, previewAtivo, tamanhoLote, resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais });
      if (temMais) {
        return { statusCode: 206, headers: { 'Content-Type': 'application/json', 'Retry-After': '1' }, body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary }) };
      }
      return respostaJson(200, { nextOffset: null, done: true, summary });
    }
  }

  // Fim (multi-lotes desligado por padrão)
  const summary = buildSummary({ endpointUrl, totalLinhas, previewAtivo, tamanhoLote, resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais });
  return respostaJson(200, { nextOffset: null, done: true, summary });
}

module.exports = { executarEnvioDireto };

// src/services/envioDiretoService.js
const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');

const LAMBDA_TIMEOUT_MS = 25000;

// Helpers de status
const getStatus = (r) => Number(r?.statusCode ?? r?.status ?? r?.code ?? 0);
const isOk = (r) => {
  const sc = getStatus(r);
  return sc >= 200 && sc < 300;
};

// Modo de aceitação e modo 1-lote
const ACCEPT_MODE = String(process.env.ACCEPT_MODE || 'optimistic').toLowerCase();
const SINGLE_BATCH_MODE = String(process.env.SINGLE_BATCH_MODE ?? '1') === '1';

// Normalizadores de contadores vindos da API
const _num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

function extractCounts(body) {
  const inserted =
    _num(body?.recordsInserted) ||
    _num(body?.data?.recordsInserted) ||
    _num(body?.summary?.recordsInserted) ||
    (Array.isArray(body?.records) ? body.records.length : 0) ||
    _num(body?.received) ||
    _num(body?.accepted);

  const updated = _num(body?.recordsUpdated);
  const deleted = _num(body?.recordsDeleted);

  return { inserted, updated, deleted };
}

function countErrors(arr) {
  if (ACCEPT_MODE === 'optimistic') {
    // Só conta falhas de transporte
    return arr.filter((r) => {
      const sc = getStatus(r);
      return !sc || sc === 599;
    }).length;
  }
  // strict: qualquer não-2xx
  return arr.filter((r) => !isOk(r)).length;
}

function buildSummary({
  endpointUrl,
  totalLinhas,
  previewAtivo,
  tamanhoLote,
  resultados,
  uploadId,
  fileHash,
  iniciouEm,
  offsetInicial,
  indiceAceito,
  totais
}) {
  const duracao = Date.now() - iniciouEm;

  // Fonte única da verdade: counters confirmados pela API
  const confirmados =
    (totais.recordsInserted + totais.recordsUpdated + totais.recordsDeleted) ||
    Math.max(0, indiceAceito - offsetInicial); // fallback

  return {
    modo: 'direto-flowch',
    endpointUrl,
    linhasLidas: totalLinhas,
    enviadasAprox: confirmados,
    errosBatches: countErrors(resultados),
    duracaoMs: duracao,
    dryRun: false,
    preview: !!previewAtivo,
    batchSize: tamanhoLote,
    totalBatches: resultados.length,
    uploadId,
    fileHash,
    size: confirmados,
    recordsInserted: totais.recordsInserted,
    recordsUpdated: totais.recordsUpdated,
    recordsDeleted: totais.recordsDeleted
  };
}

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

  // (opcionais) ajustes do preditor via handler/env
  etaAlpha,
  etaMultiplier,
  etaMinMs
}) {
  const tamanhoLote = validarBatchSize(batchSize, 20);

  // Preditor (EMA) – suavização de tempo por lote
  const alpha = Math.max(0.05, Math.min(0.95, Number(process.env.ETA_ALPHA ?? etaAlpha ?? 0.4)));
  const mult = Math.max(1, Number(process.env.ETA_MULTIPLIER ?? etaMultiplier ?? 1.1));
  const etaMin = Math.max(100, Number(process.env.ETA_MIN_MS ?? etaMinMs ?? 300));
  let etaAvgMs = 0;
  let etaLastMs = 0;
  const etaEstimate = () => Math.max(etaMin, (etaAvgMs || etaLastMs || etaMin)) * mult;
  const etaUpdate = (ms) => {
    etaLastMs = ms;
    etaAvgMs = etaAvgMs ? alpha * ms + (1 - alpha) * etaAvgMs : ms;
  };

  // Dry-run
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
        recordsDeleted: 0
      }
    });
  }

  const registrosProcessados = registros || [];
  let indice = Math.min(offsetInicial, registrosProcessados.length);

  // Acumuladores
  const resultados = [];
  const totais = { recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };
  let aceitosTotais = 0; // soma confirmada pelo servidor nesta invocação
  let indiceAceito = offsetInicial;

  while (indice < registrosProcessados.length) {
    // Orçamento do API Gateway
    const gwLimitMs = Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000;
    const elapsed = Date.now() - iniciouEm;
    const budgetAntesDoLote = gwLimitMs - margemSegurancaMs - elapsed;

    // ⚠️ Nunca retornar 206 antes de enviar o PRIMEIRO lote desta invocação
    const isPrimeiroLoteDestaInvocacao = resultados.length === 0;
    if (!isPrimeiroLoteDestaInvocacao) {
      const precisoMs = etaEstimate();
      if (budgetAntesDoLote <= precisoMs || budgetAntesDoLote <= 0) {
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({
            nextOffset: indiceAceito,
            done: false,
            summary: buildSummary({
              endpointUrl,
              totalLinhas,
              previewAtivo,
              tamanhoLote,
              resultados,
              uploadId,
              fileHash,
              iniciouEm,
              offsetInicial,
              indiceAceito,
              totais
            })
          })
        };
      }
    }

    const fim = Math.min(indice + tamanhoLote, registrosProcessados.length);
    const fatia = registrosProcessados.slice(indice, fim);
    if (fatia.length === 0) break;

    const timeoutBase = Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS;
    const overheadMs = Math.max(50, Number(process.env.REQ_OVERHEAD_MS || 200));
    const budgetParaRequisicao = Math.max(1000, budgetAntesDoLote - overheadMs);
    const effectiveTimeoutMs = Math.max(800, Math.min(timeoutBase, budgetParaRequisicao)); // garante 1ª tentativa

    const t0 = Date.now();
    const agregado = await sendBatchesDirectToFlowch({
      endpointUrl,
      token,
      records: fatia,
      batchSize: tamanhoLote,
      timeoutMs: effectiveTimeoutMs,
      method: 'POST'
    });
    etaUpdate(Date.now() - t0);
    resultados.push(...agregado.results);

    // Somar com base no que o SERVIDOR confirmou
    let aceitosNoCiclo = 0;
    for (const resultado of agregado.results) {
      let body = resultado.body || {};
      if (typeof body === 'string') {
        const s = body.trim();
        if (s && (s[0] === '{' || s[0] === '[')) {
          try { body = JSON.parse(s); } catch { body = {}; }
        } else {
          body = {};
        }
      }

      const { inserted, updated, deleted } = extractCounts(body);
      const byCounters = inserted + updated + deleted;

      let aceitosEste = 0;
      if (byCounters > 0) {
        aceitosEste = byCounters;
        totais.recordsInserted += inserted;
        totais.recordsUpdated += updated;
        totais.recordsDeleted += deleted;
      } else {
        // último fallback: tamanho do lote enviado
        aceitosEste = Number(resultado.size || 0);
        totais.recordsInserted += aceitosEste;
      }
      aceitosNoCiclo += aceitosEste;
    }

    aceitosTotais += aceitosNoCiclo;
    indiceAceito = offsetInicial + aceitosTotais;

    // Cursor local segue leitura sequencial do arquivo
    indice = fim;

    // 1 lote por invocação (padrão)
    if (SINGLE_BATCH_MODE) {
      const temMais = fim < registrosProcessados.length;
      const summary = buildSummary({
        endpointUrl,
        totalLinhas,
        previewAtivo,
        tamanhoLote,
        resultados,
        uploadId,
        fileHash,
        iniciouEm,
        offsetInicial,
        indiceAceito,
        totais
      });
      if (temMais) {
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary })
        };
      }
      return respostaJson(200, { nextOffset: null, done: true, summary });
    }

    // (Multi-lotes: respeita orçamento)
    const gwElapsed = Date.now() - iniciouEm;
    const deveEncerrar =
      gwElapsed >= (Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000) - margemSegurancaMs;

    if (deveEncerrar) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: indiceAceito,
          done: false,
          summary: buildSummary({
            endpointUrl,
            totalLinhas,
            previewAtivo,
            tamanhoLote,
            resultados,
            uploadId,
            fileHash,
            iniciouEm,
            offsetInicial,
            indiceAceito,
            totais
          })
        })
      };
    }
  }

  // Conclusão
  const summary = buildSummary({
    endpointUrl,
    totalLinhas,
    previewAtivo,
    tamanhoLote,
    resultados,
    uploadId,
    fileHash,
    iniciouEm,
    offsetInicial,
    indiceAceito,
    totais
  });

  return respostaJson(200, { nextOffset: null, done: true, summary });
}

module.exports = { executarEnvioDireto };

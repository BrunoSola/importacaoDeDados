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

function buildSummary({
  endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
  resultados, uploadId, fileHash, iniciouEm,
  offsetInicial, indiceAceito, totais
}) {
  const duracao = Date.now() - iniciouEm;
  const enviadosConfirmados = Math.max(0, indiceAceito - offsetInicial); // fonte única da verdade
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
    recordsUpdated:  totais.recordsUpdated,
    recordsDeleted:  totais.recordsDeleted,
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
  etaMinMs,
}) {
  const tamanhoLote = validarBatchSize(batchSize, 20);

  // Preditor de tempo por lote (EMA)
  const alpha = Math.max(0.05, Math.min(0.95, Number(process.env.ETA_ALPHA ?? etaAlpha ?? 0.4)));
  const mult  = Math.max(1, Number(process.env.ETA_MULTIPLIER ?? etaMultiplier ?? 1.1));
  const etaMin = Math.max(100, Number(process.env.ETA_MIN_MS ?? etaMinMs ?? 300));
  let etaAvgMs = 0; // média móvel exponencial
  let etaLastMs = 0; // última medição
  const etaEstimate = () => {
    const base = etaAvgMs || etaLastMs || etaMin;
    return Math.max(etaMin, base) * mult;
  };
  const etaUpdate = (ms) => {
    etaLastMs = ms;
    etaAvgMs = etaAvgMs ? (alpha * ms + (1 - alpha) * etaAvgMs) : ms;
  };

  // DRY-RUN
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

  const registrosProcessados = registros || [];
  let indice = Math.min(offsetInicial, registrosProcessados.length);

  // Acumuladores
  const resultados = [];
  const totais = { recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };
  let tentadosTotais = 0;   // enviados ao endpoint (tentativas)
  let aceitosTotais = 0;    // aceitos nesta invocação (base para nextOffset)
  let indiceAceito = offsetInicial; // offset real (baseado no que entrou)

  while (indice < registrosProcessados.length) {
    // Orçamento do API GW (soft timeout)
    const gwLimitMs = Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000;
    const elapsed = Date.now() - iniciouEm;
    const budgetAntesDoLote = gwLimitMs - margemSegurancaMs - elapsed;

    // Previsão: se já temos medida de 1+ lotes, decide se cabe outro
    if (aceitosTotais > 0) {
      const precisoMs = etaEstimate();
      if (budgetAntesDoLote <= precisoMs) {
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({
            nextOffset: indiceAceito,
            done: false,
            summary: buildSummary({
              endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
              resultados, uploadId, fileHash, iniciouEm,
              offsetInicial, indiceAceito, totais
            }),
          }),
        };
      }
    }

    // Guarda “hard” do orçamento
    if (budgetAntesDoLote <= 0) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: indiceAceito,
          done: false,
          summary: buildSummary({
            endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
            resultados, uploadId, fileHash, iniciouEm,
            offsetInicial, indiceAceito, totais
          }),
        }),
      };
    }

    const fim = Math.min(indice + tamanhoLote, registrosProcessados.length);
    const fatia = registrosProcessados.slice(indice, fim);
    if (fatia.length === 0) break;

    // Timeout efetivo desta requisição (respeita o budget)
    const timeoutBase = Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS;
    const overheadMs = Math.max(50, Number(process.env.REQ_OVERHEAD_MS || 200));
    const budgetParaRequisicao = Math.max(1000, budgetAntesDoLote - overheadMs);
    const effectiveTimeoutMs = Math.min(timeoutBase, budgetParaRequisicao);

    tentadosTotais += fatia.length;

    const t0 = Date.now();
    const agregado = await sendBatchesDirectToFlowch({
      endpointUrl,
      token,
      records: fatia,
      batchSize: tamanhoLote,
      timeoutMs: effectiveTimeoutMs,
      method: 'POST',
    });
    etaUpdate(Date.now() - t0);

    resultados.push(...agregado.results);

    // Acumula métricas por resultado e computa ACEITOS no ciclo
    let aceitosNoCiclo = 0;
    for (const resultado of agregado.results) {
      // modo "optimistic": conta enviado mesmo sem counters confiáveis
      const ok = isOk(resultado);

      let respBody = resultado.body || {};
      if (typeof respBody === 'string') {
        const t = respBody.trim();
        if (t && (t[0] === '{' || t[0] === '[')) { try { respBody = JSON.parse(t); } catch {} }
      }

      const inserted = Number(respBody.recordsInserted || 0);
      const updated  = Number(respBody.recordsUpdated  || 0);
      const deleted  = Number(respBody.recordsDeleted  || 0);
      const byCounters = inserted + updated + deleted;

      // compat extra: quando não há contadores oficiais, usar received/accepted; se não houver, usar o tamanho do lote
      let compatAceitos = 0;
      if (byCounters === 0) {
        const received = Number(respBody.received ?? respBody.accepted ?? 0);
        compatAceitos = Number.isFinite(received) && received > 0 ? received : Number(resultado.size || 0);
      }

      let aceitosEste = 0;
      if (ACCEPT_MODE === 'optimistic') {
        aceitosEste = byCounters > 0 ? byCounters : Number(resultado.size || 0);
        if (byCounters > 0) {
          totais.recordsInserted += inserted;
          totais.recordsUpdated  += updated;
          totais.recordsDeleted  += deleted;
        } else {
          totais.recordsInserted += Number(resultado.size || 0);
        }
      } else {
        if (ok) {
          aceitosEste = byCounters > 0 ? byCounters : compatAceitos;
          if (byCounters > 0) {
            totais.recordsInserted += inserted;
            totais.recordsUpdated  += updated;
            totais.recordsDeleted  += deleted;
          } else {
            totais.recordsInserted += compatAceitos;
          }
        }
      }

      aceitosNoCiclo += aceitosEste;
    }

    aceitosTotais += aceitosNoCiclo;
    indiceAceito = offsetInicial + aceitosTotais; // offset real: só o que entrou

    // Cursor local de varredura (independente do aceito)
    indice = fim;

    // === SINGLE-BATCH MODE: sai após o 1º lote ===
    if (SINGLE_BATCH_MODE) {
      const temMais = fim < registrosProcessados.length;
      const summary = buildSummary({
        endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
        resultados, uploadId, fileHash, iniciouEm,
        offsetInicial, indiceAceito, totais
      });

      if (temMais) {
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary }),
        };
      }
      return respostaJson(200, { nextOffset: null, done: true, summary });
    }

    // Encerramento por orçamento de Lambda ou API GW (modo multi-lotes)
    const lambdaRemaining = (context && typeof context.getRemainingTimeInMillis === 'function')
      ? context.getRemainingTimeInMillis()
      : ((Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS) - elapsed);

    const gwElapsed = Date.now() - iniciouEm;
    const deveEncerrar = (lambdaRemaining <= margemSegurancaMs) || (gwElapsed >= (gwLimitMs - margemSegurancaMs));

    if (deveEncerrar) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: indiceAceito,
          done: false,
          summary: buildSummary({
            endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
            resultados, uploadId, fileHash, iniciouEm,
            offsetInicial, indiceAceito, totais
          }),
        }),
      };
    }
  }

  // Finalizou a varredura deste conjunto (modo multi-lotes desligado por padrão)
  const amostrasErro = [];
  for (const r of resultados) {
    const isError = (ACCEPT_MODE === 'optimistic') ? (!getStatus(r) || getStatus(r) === 599) : !isOk(r);
    if (isError && amostrasErro.length < 5) {
      const bodyText = typeof r.body === 'string' ? r.body : JSON.stringify(r.body);
      amostrasErro.push({ statusCode: getStatus(r), snippet: String(bodyText).slice(0, 400) });
    }
  }

  const summary = buildSummary({
    endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
    resultados, uploadId, fileHash, iniciouEm,
    offsetInicial, indiceAceito, totais
  });

  return respostaJson(200, {
    nextOffset: null,
    done: true,
    summary: { ...summary, errorSamples: amostrasErro },
  });
}

module.exports = { executarEnvioDireto };

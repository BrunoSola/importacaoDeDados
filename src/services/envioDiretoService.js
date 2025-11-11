// src/services/envioDiretoService.js
const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');

const LAMBDA_TIMEOUT_MS = 25000;
const SINGLE_BATCH_MODE = String(process.env.SINGLE_BATCH_MODE ?? '1') === '1';
const ACCEPT_MODE = String(process.env.ACCEPT_MODE || 'optimistic').toLowerCase();

// --------- helpers bem diretos ---------
const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const safeParse = (maybeJson) => {
  if (typeof maybeJson !== 'string') return (maybeJson || {});
  const s = maybeJson.trim();
  if (!s || (s[0] !== '{' && s[0] !== '[')) return {};
  try { return JSON.parse(s); } catch { return {}; }
};

const getStatus = (r) => Number(r?.statusCode ?? r?.status ?? r?.code ?? 0);
const isOk      = (r) => { const sc = getStatus(r); return sc >= 200 && sc < 300; };

const countErrors = (arr) => {
  if (ACCEPT_MODE === 'optimistic') {
    // só falhas grosseiras de transporte
    return arr.filter((r) => {
      const sc = getStatus(r);
      return !sc || sc === 599;
    }).length;
  }
  // strict: qualquer não-2xx
  return arr.filter((r) => !isOk(r)).length;
};

/**
 * Extrai contadores do corpo devolvido pela API.
 * Prioriza recordsInserted/Updated/Deleted; se inclusão devolver "records: [{Id}]",
 * usamos o length como inserted.
 */
function extractCounts(bodyRaw) {
  const body = safeParse(bodyRaw);
  const inserted =
    num(body.recordsInserted) ||
    num(body?.data?.recordsInserted) ||
    num(body?.summary?.recordsInserted) ||
    (Array.isArray(body.records) ? body.records.length : 0) ||
    num(body.received) || // compat
    num(body.accepted);   // compat

  const updated = num(body.recordsUpdated);
  const deleted = num(body.recordsDeleted);
  return { inserted, updated, deleted, body };
}

/**
 * Monta o summary padronizado.
 * "enviadasAprox" e "size" são os confirmados (inserted+updated+deleted).
 */
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
  totais,
}) {
  const duracao = Date.now() - iniciouEm;
  const confirmados =
    (totais.recordsInserted + totais.recordsUpdated + totais.recordsDeleted) ||
    Math.max(0, indiceAceito - offsetInicial);

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
    recordsUpdated:  totais.recordsUpdated,
    recordsDeleted:  totais.recordsDeleted,
  };
}

/**
 * Calcula orçamento de tempo para o request atual, com fallback seguro.
 */
function calcEffectiveTimeout({ iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs }) {
  const gwLimitMs = Number.isFinite(apigwSoftTimeoutMs) ? apigwSoftTimeoutMs : 29000;
  const msSafe    = Number.isFinite(margemSegurancaMs) ? margemSegurancaMs : Number(process.env.SAFE_REMAINING_MS || 4000);
  const elapsed   = Date.now() - iniciouEm;
  const budget    = gwLimitMs - msSafe - elapsed;

  const timeoutBase        = Number.isFinite(timeoutMs) ? timeoutMs : LAMBDA_TIMEOUT_MS;
  const overheadMs         = Math.max(50, Number(process.env.REQ_OVERHEAD_MS || 200));
  const budgetRequisicao   = Math.max(1000, budget - overheadMs);
  const effectiveTimeoutMs = Math.max(800, Math.min(timeoutBase, budgetRequisicao));

  return { effectiveTimeoutMs, budget };
}

// --------- função principal, fluxo linear e comentado ---------
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
  // (opcionais) ajustes do preditor via handler/env (mantidos para compat)
  etaAlpha,
  etaMultiplier,
  etaMinMs,
}) {
  const tamanhoLote = num(batchSize) > 0 ? Number(batchSize) : 20;

  // DRY-RUN: só devolve o esqueleto
  if (dryRun) {
    const summary = {
      modo: 'direto-flowch',
      endpointUrl,
      linhasLidas: totalLinhas,
      enviadasAprox: 0,
      errosBatches: 0,
      duracaoMs: Date.now() - iniciouEm,
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
    };
    return respostaJson(200, { nextOffset: offsetInicial, done: true, summary });
  }

  // Cursor de leitura no arquivo/matriz
  const todos = Array.isArray(registros) ? registros : [];
  let indice   = Math.min(offsetInicial || 0, todos.length);

  // Acumuladores desta invocação
  const resultados = [];
  const totais     = { recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };
  let aceitosTotais   = 0;
  let indiceAceito    = offsetInicial || 0;

  // === enviamos APENAS 1 LOTE por invocação (SINGLE_BATCH_MODE padrão) ===
  // Ainda deixamos num while pra manter compat se mudar a flag futuramente.
  while (indice < todos.length) {
    // fatia do lote
    const fim   = Math.min(indice + tamanhoLote, todos.length);
    const fatia = todos.slice(indice, fim);
    if (fatia.length === 0) break;

    // calcular timeout efetivo com base no budget restante
    const { effectiveTimeoutMs, budget } = calcEffectiveTimeout({
      iniciouEm,
      apigwSoftTimeoutMs,
      margemSegurancaMs,
      timeoutMs,
    });

    // GARANTIA: no primeiro lote da invocação, enviamos mesmo com pouco budget
    // (o effectiveTimeout já respeita piso).
    const agregado = await sendBatchesDirectToFlowch({
      endpointUrl,
      token,
      records: fatia,
      batchSize: tamanhoLote,
      timeoutMs: effectiveTimeoutMs,
      method: 'POST',
    });

    // proteger contra retorno vazio (ex.: erro de rede encapsulado)
    if (!agregado?.results || agregado.results.length === 0) {
      const summary = buildSummary({
        endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
        resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais,
      });
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary }),
      };
    }

    // acumular contadores vindos do servidor
    resultados.push(...agregado.results);
    let aceitosNoLote = 0;

    for (const r of agregado.results) {
      const { inserted, updated, deleted } = extractCounts(r.body);
      const byCounters = inserted + updated + deleted;

      if (byCounters > 0) {
        aceitosNoLote                += byCounters;
        totais.recordsInserted       += inserted;
        totais.recordsUpdated        += updated;
        totais.recordsDeleted        += deleted;
      } else {
        // fallback: se o servidor não informou contadores, usamos tamanho do lote
        const fallback = num(r.size);
        aceitosNoLote          += fallback;
        totais.recordsInserted += fallback;
      }
    }

    aceitosTotais += aceitosNoLote;
    indiceAceito   = (offsetInicial || 0) + aceitosTotais;

    // avançar leitura do arquivo (os não-aceitos serão reprocessados na próxima chamada)
    indice = SINGLE_BATCH_MODE ? fim : Math.max(indiceAceito, indice);

    // encerramento (1 lote por execução)
    const temMais = fim < todos.length;
    const summary = buildSummary({
      endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
      resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais,
    });

    if (SINGLE_BATCH_MODE) {
      if (temMais) {
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary }),
        };
      }
      return respostaJson(200, { nextOffset: null, done: true, summary });
    }

    // (multi-lotes): só continua se houver budget; senão, devolve 206
    if (budget <= 0) {
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({ nextOffset: indiceAceito, done: false, summary }),
      };
    }
  }

  // terminou tudo
  const summary = buildSummary({
    endpointUrl, totalLinhas, previewAtivo, tamanhoLote,
    resultados, uploadId, fileHash, iniciouEm, offsetInicial, indiceAceito, totais,
  });
  return respostaJson(200, { nextOffset: null, done: true, summary });
}

module.exports = { executarEnvioDireto };

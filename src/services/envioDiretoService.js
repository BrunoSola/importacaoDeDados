// src/services/envioDiretoService.js
//
// Envio DIRETO → Flowch com tratamento ADAPTATIVO de duplicatas, sem usar serviços pagos.
// Regras principais (em português):
// - x-batch-size: tamanho nominal e estável escolhido pelo cliente (se 0/≤0, usa o padrão do código).
// - x-suggest-batch-size: tamanho ADAPTATIVO sugerido. 0 = desativado; >0 substitui o nominal temporariamente.
// - Em erro 400 (duplicatas) ou orçamento de tempo curto, dividimos o lote (metade/metade) até isolar itens:
//   * Se lote inteiro for duplicado → pulamos todos como duplicados (skippedDuplicates) e avançamos o offset.
//   * Se for 1 item e continuar falhando → pulamos 1 e avançamos.
// - Progresso SEMPRE: nextOffset avança por (aceitos + pulados). Se não houve progresso → devolvemos 206 com
//   nextOffset igual ao offsetInicial e x-suggest-batch-size com uma sugestão menor (metade do efetivo).
// - Quando um lote passa “limpo” (sem subdivisão), devolvemos x-suggest-batch-size = 0 (volta a usar o nominal).
//
// Observação:
// - Mantém API/shape da resposta; apenas adiciona cabeçalhos e campos em summary para melhor telemetria.
// - SINGLE_BATCH_MODE='1' (padrão): 1 lote por invocação — seguro para janela ~29s do API Gateway.

const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');

const LAMBDA_TIMEOUT_MS = 25000;
const SINGLE_BATCH_MODE = String(process.env.SINGLE_BATCH_MODE ?? '1') === '1';
const ACCEPT_MODE = String(process.env.ACCEPT_MODE || 'optimistic').toLowerCase();
const RETRY_AFTER_SECS = Math.max(0, Number(process.env.RETRY_AFTER_SECS || 1));

/** Converte para número com fallback 0 */
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

/** Faz parse de JSON apenas se parecer JSON (evita custo desnecessário) */
const safeParseIfJson = (maybeJson) => {
  if (typeof maybeJson !== 'string') return (maybeJson || {});
  const s = maybeJson.trim();
  const c = s.charCodeAt(0); // 123:'{', 91:'['
  if (c !== 123 && c !== 91) return {};
  try { return JSON.parse(s); } catch { return {}; }
};

const getStatus = (r) => Number(r?.statusCode ?? r?.status ?? r?.code ?? 0);
const isOk      = (r) => { const sc = getStatus(r); return sc >= 200 && sc < 300; };

/** Política de erro por item (telemetria) */
const isCountedAsError = (r) => {
  if (ACCEPT_MODE === 'optimistic') {
    const sc = getStatus(r);
    return !sc || sc === 599;
  }
  return !isOk(r);
};

/** Extrai contadores de alteração e sinais de duplicata do corpo da resposta */
function extractCounts(bodyRaw) {
  const body = safeParseIfJson(bodyRaw);

  const inserted =
    num(body.recordsInserted) ||
    num(body?.data?.recordsInserted) ||
    num(body?.summary?.recordsInserted) ||
    (Array.isArray(body.records) ? body.records.length : 0) ||
    num(body.received) ||
    num(body.accepted);

  const updated = num(body.recordsUpdated);
  const deleted = num(body.recordsDeleted);

  // Heurísticas de duplicata: somamos indicadores diretos e erros ALREADY_EXISTS
  let alreadyExists = 0;
  const sumLike = (...vals) => vals.reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);

  alreadyExists += sumLike(
    num(body.alreadyExists),
    num(body.duplicates),
    num(body.skippedDuplicates),
    num(body?.data?.alreadyExists),
    num(body?.summary?.alreadyExists),
  );

  const errorsArr = Array.isArray(body.errors) ? body.errors
                  : Array.isArray(body?.data?.errors) ? body.data.errors
                  : [];
  for (const e of errorsArr) {
    const code = String(e?.code || '').toUpperCase();
    const msg  = String(e?.message || '').toLowerCase();
    if (code === 'ALREADY_EXISTS' || msg.includes('already_exist') || msg.includes('já cadastr') || msg.includes('duplic')) {
      alreadyExists += 1;
    }
  }

  return { inserted, updated, deleted, alreadyExists, body };
}

/** Retorna true quando TODO o lote é duplicado (mudanças=0 e dups == enviados) */
function isWholeBatchDuplicate({ sent, counts, statusCode }) {
  const totalChanges = counts.inserted + counts.updated + counts.deleted;
  if (totalChanges > 0) return false;
  if (statusCode === 400 && counts.alreadyExists >= sent && sent > 0) return true;
  return (counts.alreadyExists >= sent && sent > 0);
}

/** Monta o resumo da execução (sem re-varrer estruturas) */
function buildSummary({
  endpointUrl,
  totalLinhas,
  previewAtivo,
  tamanhoLoteEfetivo,
  uploadId,
  fileHash,
  iniciouEm,
  offsetInicial,
  indiceAceito,
  totais,
  errosBatchesCount,
  totalBatches,
  skippedDuplicates,
  batchNominal,
  batchSugerido,
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
    errosBatches: errosBatchesCount,
    duracaoMs: duracao,
    dryRun: false,
    preview: !!previewAtivo,

    // Informações de batch
    batchSize: batchNominal,                 // nominal do cliente (x-batch-size)
    suggestBatchSize: batchSugerido,         // valor recebido em x-suggest-batch-size
    effectiveBatchSize: tamanhoLoteEfetivo,  // usado nesta execução (sugerido>0 ? sugerido : nominal)
    totalBatches,

    uploadId,
    fileHash,
    size: confirmados,
    recordsInserted: totais.recordsInserted,
    recordsUpdated:  totais.recordsUpdated,
    recordsDeleted:  totais.recordsDeleted,
    skippedDuplicates,                       // quantos itens foram pulados por duplicata
  };
}

/** Calcula orçamento/timeout efetivo para a tentativa atual (seguro para janela do APIGW) */
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

/**
 * Envia um lote com “consciência de duplicata” (divide&conquista).
 * - Se todo o lote for duplicado: pulamos todos (skippedDuplicates = sent).
 * - Se 1 item falhar: pulamos 1 (garante progresso).
 * - Se houver mudanças confirmadas (insert/update/delete): aceitamos.
 * Retorna métricas do envio e se houve uso de split adaptativo.
 */
async function sendDuplicateAware({
  records, startIndex,
  endpointUrl, token, batchSizeEfetivo,
  iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs,
}) {
  const sent = records.length;
  if (sent === 0) {
    return { accepted: 0, inserted: 0, updated: 0, deleted: 0, skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 0, budgetLeft: null, usedAdaptiveSplit: false };
  }

  const { effectiveTimeoutMs, budget } = calcEffectiveTimeout({ iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs });
  if (budget <= 0) {
    return { accepted: 0, inserted: 0, updated: 0, deleted: 0, skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 0, budgetLeft: budget, usedAdaptiveSplit: true };
  }

  const aggregated = await sendBatchesDirectToFlowch({
    endpointUrl,
    token,
    records,
    batchSize: Math.max(1, Math.min(batchSizeEfetivo, sent)),
    timeoutMs: effectiveTimeoutMs,
    method: 'POST',
  });

  if (!aggregated?.results || aggregated.results.length === 0) {
    return { accepted: 0, inserted: 0, updated: 0, deleted: 0, skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 1, budgetLeft: budget, usedAdaptiveSplit: true };
  }

  let inserted = 0, updated = 0, deleted = 0;
  let skippedDuplicates = 0;
  let errosBatchesCountDelta = 0;

  for (const r of aggregated.results) {
    if (isCountedAsError(r)) errosBatchesCountDelta += 1;
    const counts = extractCounts(r.body);
    inserted += counts.inserted;
    updated  += counts.updated;
    deleted  += counts.deleted;
  }

  const anyOk = aggregated.results.some(isOk);
  const firstStatus = getStatus(aggregated.results[0]);
  const mergedCounts = extractCounts(aggregated.results[0]?.body || '{}');

  // Caso feliz: houve mudanças confirmadas
  if (anyOk && (inserted + updated + deleted) > 0) {
    return {
      accepted: inserted + updated + deleted,
      inserted, updated, deleted,
      skippedDuplicates,
      errosBatchesCountDelta,
      batchesUsed: 1,
      budgetLeft: budget,
      usedAdaptiveSplit: false, // passou “limpo”
    };
  }

  // Lote inteiro duplicado → pula tudo (progresso garantido)
  if (firstStatus === 400 && isWholeBatchDuplicate({ sent, counts: mergedCounts, statusCode: 400 })) {
    return {
      accepted: 0,
      inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: sent,
      errosBatchesCountDelta,
      batchesUsed: 1,
      budgetLeft: budget,
      usedAdaptiveSplit: true,
    };
  }

  // 1 item e falhou → pular 1 (evita loop)
  if (sent === 1) {
    return {
      accepted: 0,
      inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 1,
      errosBatchesCountDelta,
      batchesUsed: 1,
      budgetLeft: budget,
      usedAdaptiveSplit: true,
    };
  }

  // Divide&Conquista
  const mid = Math.floor(sent / 2);
  const left  = records.slice(0, mid);
  const right = records.slice(mid);

  const leftRes = await sendDuplicateAware({
    records: left, startIndex,
    endpointUrl, token, batchSizeEfetivo,
    iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs,
  });

  const rightRes = await sendDuplicateAware({
    records: right, startIndex: startIndex + mid,
    endpointUrl, token, batchSizeEfetivo,
    iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs,
  });

  return {
    accepted: leftRes.accepted + rightRes.accepted,
    inserted: leftRes.inserted + rightRes.inserted,
    updated:  leftRes.updated  + rightRes.updated,
    deleted:  rightRes.deleted + leftRes.deleted,
    skippedDuplicates: leftRes.skippedDuplicates + rightRes.skippedDuplicates,
    errosBatchesCountDelta: errosBatchesCountDelta + leftRes.errosBatchesCountDelta + rightRes.errosBatchesCountDelta,
    batchesUsed: 1 + leftRes.batchesUsed + rightRes.batchesUsed,
    budgetLeft: rightRes.budgetLeft ?? leftRes.budgetLeft,
    usedAdaptiveSplit: leftRes.usedAdaptiveSplit || rightRes.usedAdaptiveSplit || true,
  };
}

// --------- Função principal: aplica política de batch nominal vs sugerido ---------
async function executarEnvioDireto({
  registros,
  offsetInicial,
  endpointUrl,
  token,
  batchSize,           // nominal (pode vir 0/≤0)
  batchSizeSugerido,   // sugerido (0 desativa)
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
  // compat reservados
  etaAlpha,
  etaMultiplier,
  etaMinMs,
}) {
  // Padrão interno do serviço: se x-batch-size ≤ 0, usamos BATCH_SIZE/env ou 20.
  const defaultNominal = Math.max(1, Number(process.env.BATCH_SIZE || 20));

  const batchNominal =
    Number.isFinite(batchSize) && batchSize > 0 ? Number(batchSize) : defaultNominal;

  // Sugerido só vale se > 0
  const batchSugerido =
    Number.isFinite(batchSizeSugerido) && batchSizeSugerido > 0 ? Number(batchSizeSugerido) : 0;

  // Efetivo = sugerido (se ativo), senão nominal
  const batchEfetivo = batchSugerido > 0 ? batchSugerido : batchNominal;

  // DRY-RUN: não envia nada; útil para validar cabeçalhos
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
      batchSize: batchNominal,
      suggestBatchSize: batchSugerido,
      effectiveBatchSize: batchEfetivo,
      totalBatches: 0,
      uploadId,
      fileHash,
      size: 0,
      recordsInserted: 0,
      recordsUpdated: 0,
      recordsDeleted: 0,
      skippedDuplicates: 0,
    };
    return respostaJson(200, { nextOffset: offsetInicial, done: true, summary });
  }

  const todos = Array.isArray(registros) ? registros : [];
  let indice        = Math.min(offsetInicial || 0, todos.length);
  let indiceAceito  = offsetInicial || 0;
  let aceitosTotais = 0;

  // Acumuladores desta invocação
  const totais = { recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0 };
  let errosBatchesCount = 0;
  let totalBatches = 0;
  let skippedDuplicates = 0;

  while (indice < todos.length) {
    // Fatia conforme batch EFETIVO (sugerido>0 ? sugerido : nominal)
    const fim   = Math.min(indice + batchEfetivo, todos.length);
    const fatia = todos.slice(indice, fim);
    if (fatia.length === 0) break;

    // Envio com “consciência de duplicata” (pode subdividir internamente)
    const res = await sendDuplicateAware({
      records: fatia,
      startIndex: indice,
      endpointUrl, token, batchSizeEfetivo: batchEfetivo,
      iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs,
    });

    // Consolida métricas
    totais.recordsInserted += res.inserted;
    totais.recordsUpdated  += res.updated;
    totais.recordsDeleted  += res.deleted;
    errosBatchesCount      += res.errosBatchesCountDelta;
    totalBatches           += Math.max(1, res.batchesUsed);
    skippedDuplicates      += res.skippedDuplicates;

    const progressoFatia = res.accepted + res.skippedDuplicates;
    aceitosTotais += progressoFatia;
    indiceAceito   = (offsetInicial || 0) + aceitosTotais;

    // nextOffset de saída: se não houve progresso, mantém o offsetInicial (evita “pular” pendentes)
    const nextOffsetOut = (progressoFatia > 0) ? indiceAceito : (offsetInicial || 0);

    // Avança cursor interno (não afeta próxima invocação — ela lê nextOffsetOut do body)
    indice = SINGLE_BATCH_MODE ? fim : Math.max(indiceAceito, indice);

    const temMais = fim < todos.length;

    // Monta resumo
    const summary = buildSummary({
      endpointUrl,
      totalLinhas,
      previewAtivo,
      tamanhoLoteEfetivo: batchEfetivo,
      uploadId,
      fileHash,
      iniciouEm,
      offsetInicial,
      indiceAceito,
      totais,
      errosBatchesCount,
      totalBatches,
      skippedDuplicates,
      batchNominal,
      batchSugerido,
    });

    // Política para x-suggest-batch-size de retorno:
    // - Sem progresso → sugerimos metade do efetivo (próxima chamada menor).
    // - Com progresso E houve split interno → manter sugestão ativa (continua pequeno).
    // - Com progresso E sem split → reset para 0 (encerrou bloco problemático).
    let nextSuggest = batchSugerido; // default: manter como veio
    if (progressoFatia === 0) {
      nextSuggest = Math.max(1, Math.floor(batchEfetivo / 2));
    } else if (res.usedAdaptiveSplit) {
      nextSuggest = (batchSugerido > 0) ? batchSugerido : batchEfetivo;
    } else {
      nextSuggest = 0;
    }

    // Cabeçalhos sempre ecoados
    const baseHeaders = {
      'Content-Type': 'application/json',
      'x-batch-size': String(batchNominal),
      'x-suggest-batch-size': String(nextSuggest),
    };

    // SINGLE_BATCH_MODE (recomendado): 1 lote por invocação
    if (SINGLE_BATCH_MODE) {
      if (temMais) {
        return {
          statusCode: 206,
          headers: { ...baseHeaders, 'Retry-After': String(RETRY_AFTER_SECS) },
          body: JSON.stringify({
            nextOffset: nextOffsetOut,
            done: false,
            summary,
            hints: { suggestNextBatchSize: nextSuggest }, // redundância útil no body
          }),
        };
      }
      // Concluído
      return {
        statusCode: 200,
        headers: baseHeaders,
        body: JSON.stringify({ nextOffset: null, done: true, summary }),
      };
    }

    // (modo multi-lotes): orçamento estourado → 206
    if ((res.budgetLeft ?? 1) <= 0) {
      return {
        statusCode: 206,
        headers: { ...baseHeaders, 'Retry-After': String(RETRY_AFTER_SECS) },
        body: JSON.stringify({
          nextOffset: nextOffsetOut,
          done: false,
          summary,
          hints: { suggestNextBatchSize: nextSuggest },
        }),
      };
    }
  }

  // Sem mais itens
  const summary = buildSummary({
    endpointUrl,
    totalLinhas,
    previewAtivo,
    tamanhoLoteEfetivo: batchEfetivo,
    uploadId,
    fileHash,
    iniciouEm,
    offsetInicial,
    indiceAceito,
    totais,
    errosBatchesCount,
    totalBatches,
    skippedDuplicates,
    batchNominal,
    batchSugerido,
  });

  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'application/json',
      'x-batch-size': String(batchNominal),
      'x-suggest-batch-size': '0', // final “limpo”: volta ao nominal
    },
    body: JSON.stringify({ nextOffset: null, done: true, summary }),
  };
}

module.exports = { executarEnvioDireto };

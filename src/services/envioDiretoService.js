// src/services/envioDiretoService.js
//
// Envio DIRETO → Flowch com tratamento ADAPTATIVO de duplicatas, sem usar serviços pagos.
// Ideia central (resumo):
// - x-batch-size: tamanho nominal e estável escolhido por você (se for 0/ausente → usamos padrão).
// - x-suggest-batch-size: tamanho ADAPTATIVO temporário. 0 = desativado; >0 substitui o nominal.
// - Se a API devolver lote totalmente duplicado, pulamos o lote todo (sem reenvio). Se for 1 item e
//   ainda falhar, pulamos 1 e avançamos (evita loop infinito).
// - O nextOffset SEMPRE avança por (aceitos + pulados). Se não houve avanço, devolvemos 206 mantendo
//   o offset e sugerindo reduzir o batch (metade do efetivo).
// - Quando um lote passa “limpo” (sem divisão adaptativa), zeramos a sugestão (volta ao nominal).
//
// Logs:
// - LOG_RESP=1 → loga status/content-type/tamanho + preview do body (1KB) do(s) retorno(s).
// - DIRECT_DEBUG=1 → logs de fluxo (decisões, offsets, etc.).
//
// SINGLE_BATCH_MODE='1' (padrão): 1 lote por invocação — seguro para janela ~29s do API Gateway.

const { sendBatchesDirectToFlowch } = require('../utils/flowchDirectSender');
const { respostaJson } = require('../utils/httpResponse');

// ---------- Parâmetros de execução (com padrões seguros) ----------
const LAMBDA_TIMEOUT_MS = 25000;
const SINGLE_BATCH_MODE = String(process.env.SINGLE_BATCH_MODE ?? '1') === '1';
const ACCEPT_MODE = String(process.env.ACCEPT_MODE || 'optimistic').toLowerCase();
const RETRY_AFTER_SECS = Math.max(0, Number(process.env.RETRY_AFTER_SECS || 1));

// ---------- Toggles de log ----------
const LOG_RESP = String(process.env.LOG_RESP || '0') === '1';
const DIRECT_DEBUG = String(process.env.DIRECT_DEBUG || '0') === '1';
const dbg = (...args) => { if (DIRECT_DEBUG) console.log('[direct]', ...args); };

// ---------- Utilidades de conversão e parse ----------
/** Converte para número com fallback 0 */
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };

/** Faz parse de JSON apenas se parecer JSON (evita custo desnecessário) */
const safeParseIfJson = (maybeJson) => {
  if (typeof maybeJson !== 'string') return (maybeJson || {});
  const s = maybeJson.trim();
  if (!s) return {};
  const c = s.charCodeAt(0); // 123:'{', 91:'['
  if (c !== 123 && c !== 91) return {};
  try { return JSON.parse(s); } catch { return {}; }
};

const getStatus = (r) => Number(r?.statusCode ?? r?.status ?? r?.code ?? 0);
const isOk      = (r) => { const sc = getStatus(r); return sc >= 200 && sc < 300; };

/** Política de contagem de erro por item (telemetria) */
const isCountedAsError = (r) => {
  if (ACCEPT_MODE === 'optimistic') {
    const sc = getStatus(r);
    // só conta erros "grosseiros" (ex.: 599/sem status)
    return !sc || sc === 599;
  }
  // strict: qualquer não-2xx
  return !isOk(r);
};

// ---------- Extração de contadores da resposta ----------
/**
 * Lê counters típicos (recordsInserted/Updated/Deleted) e conta duplicatas
 * SOMENTE via posapp_fields_error_message (valor 'ALREADY_EXISTS').
 */
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

  // ===== Duplicatas apenas via posapp_fields_error_message =====
  let alreadyExists = 0;
  const errorsArr = Array.isArray(body.errors) ? body.errors
                  : Array.isArray(body?.data?.errors) ? body.data.errors
                  : [];

  for (const e of errorsArr) {
    const pfem = e?.erro?.posapp_fields_error_message;
    if (Array.isArray(pfem)) {
      for (const item of pfem) {
        const val = String(Object.values(item || {})[0] ?? '').toUpperCase();
        if (val === 'ALREADY_EXISTS') alreadyExists += 1;
      }
    }
  }

  return { inserted, updated, deleted, alreadyExists, body };
}

/**
 * Determina se TODO o lote foi duplicado (sem mudanças e dups == enviados).
 * Aceita status 400 (erro de validação) e também 200 com counters indicando 0 mudanças e N duplicatas.
 */
function isWholeBatchDuplicate({ sent, counts, statusCode }) {
  const totalChanges = counts.inserted + counts.updated + counts.deleted;
  if (totalChanges > 0) return false;
  if (sent <= 0) return false;
  // Se vier 400 e todos já existem → lote inteiro duplicado
  if (statusCode === 400 && counts.alreadyExists >= sent) return true;
  // Mesmo raciocínio com 200 (alguns endpoints devolvem 200 com erros no corpo)
  return (counts.alreadyExists >= sent);
}

// ---------- Montagem de resumo para a resposta ----------
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

    // Telemetria de batch
    batchSize: batchNominal,                 // nominal (x-batch-size)
    suggestBatchSize: batchSugerido,         // sugerido recebido
    effectiveBatchSize: tamanhoLoteEfetivo,  // efetivo usado (sugerido>0 ? sugerido : nominal)
    totalBatches,

    uploadId,
    fileHash,
    size: confirmados,
    recordsInserted: totais.recordsInserted,
    recordsUpdated:  totais.recordsUpdated,
    recordsDeleted:  totais.recordsDeleted,
    skippedDuplicates,                       // total de itens pulados por duplicata
  };
}

// ---------- Cálculo de orçamento/timeout seguro por chamada ----------
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

// ---------- Envio de um lote com “consciência de duplicata” (divide & conquista) ----------
/**
 * Estratégia:
 * 1) Tenta enviar a fatia inteira. Se vier mudanças confirmadas → sucesso.
 * 2) Se a API indicar “lote todo duplicado” → pula tudo (skippedDuplicates = sent).
 * 3) Se só há 1 item e falhar → pula 1 (garante progresso e evita loop).
 * 4) Caso contrário, divide em 2 e tenta recursivamente (adaptação).
 */
async function sendDuplicateAware({
  records, startIndex,
  endpointUrl, token, batchSizeEfetivo,
  iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs,
}) {
  const sent = records.length;
  if (sent === 0) {
    return {
      accepted: 0, inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 0,
      budgetLeft: null, usedAdaptiveSplit: false
    };
  }

  const { effectiveTimeoutMs, budget } = calcEffectiveTimeout({ iniciouEm, apigwSoftTimeoutMs, margemSegurancaMs, timeoutMs });
  if (budget <= 0) {
    // sem orçamento → peça para reentrar com 206 mantendo offset
    return {
      accepted: 0, inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 0,
      budgetLeft: budget, usedAdaptiveSplit: true
    };
  }

  dbg('try', { startIndex, sent, batchSizeEfetivo, effectiveTimeoutMs });

  const aggregated = await sendBatchesDirectToFlowch({
    endpointUrl,
    token,
    records,
    batchSize: Math.max(1, Math.min(batchSizeEfetivo, sent)),
    timeoutMs: effectiveTimeoutMs,
    method: 'POST',
  });

  // Logs detalhados do retorno (limitados)
  if (LOG_RESP && aggregated?.results?.length) {
    aggregated.results.slice(0, 3).forEach((r, idx) => {
      const bodyStr = typeof r.body === 'string' ? r.body : JSON.stringify(r.body || {});
      console.log('[HTTP-RESP]', {
        label: 'fast-path',
        batchIndex: idx + 1,
        status: r?.statusCode ?? r?.status ?? null,
        contentType: r?.headers?.['content-type'] || r?.headers?.['Content-Type'] || null,
        bodyLen: bodyStr.length,
        bodyPreview: bodyStr.slice(0, 1024),
      });
    });
  }
  dbg('resp', {
    resultsCount: aggregated?.results?.length || 0,
    firstStatus: aggregated?.results?.[0]?.statusCode ?? null
  });

  if (!aggregated?.results || aggregated.results.length === 0) {
    // sem resposta significativa → trate como parcela consumida do orçamento e peça reentrada
    return {
      accepted: 0, inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 0, errosBatchesCountDelta: 0, batchesUsed: 1,
      budgetLeft: budget, usedAdaptiveSplit: true
    };
  }

  // Soma counters dos itens da resposta
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
  const firstBodyStr = typeof aggregated.results[0]?.body === 'string'
    ? aggregated.results[0].body
    : JSON.stringify(aggregated.results[0]?.body || {});

  // Falha de URL (ex.: x-endpoint-url invalido): parar cedo em vez de dividir em recursao
  if (firstStatus === 599 && /invalid url/i.test(firstBodyStr || '')) {
    return {
      accepted: 0, inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 0,
      errosBatchesCountDelta: errosBatchesCountDelta + 1,
      batchesUsed: 1,
      budgetLeft: null,
      usedAdaptiveSplit: false,
      fatalError: {
        statusCode: 400,
        body: JSON.stringify({ error: 'x-endpoint-url invalido ou inacessivel', detalhe: firstBodyStr }),
        headers: {},
      },
    };
  }

  // === Erro fatal de schema (coluna inexistente etc.) ===
  const first = aggregated.results[0];
  const parsed = safeParseIfJson(firstBodyStr);

  const e0 = parsed?.errors?.[0]?.erro || {};
  const sqlCode  = String(e0.code || '').toUpperCase();
  const sqlMsg   = String(e0.sqlMessage || '').toLowerCase();
  const sqlState = String(e0.sqlState || '');

  // Regras fatais: ER_BAD_FIELD_ERROR / "unknown column" / 42S22 (MySQL)
  const isSchemaError =
    sqlCode === 'ER_BAD_FIELD_ERROR' ||
    sqlState === '42S22' ||
    sqlMsg.includes('unknown column');

  const totalChanges = mergedCounts.inserted + mergedCounts.updated + mergedCounts.deleted;
  const houveProgresso = (totalChanges > 0) || (mergedCounts.alreadyExists > 0);

  if (!houveProgresso && isSchemaError) {
    return {
      accepted: 0, inserted: 0, updated: 0, deleted: 0,
      skippedDuplicates: 0,
      errosBatchesCountDelta,
      batchesUsed: 1,
      budgetLeft: null,
      usedAdaptiveSplit: false,
      fatalError: {
        statusCode: 400,
        body: firstBodyStr,
        headers: first?.headers || {},
      },
    };
  }

  // Caso feliz: houve mudanças confirmadas
  if (anyOk && (inserted + updated + deleted) > 0) {
    dbg('decision', { kind: 'accepted', changes: inserted + updated + deleted });
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
  if (isWholeBatchDuplicate({ sent, counts: mergedCounts, statusCode: firstStatus })) {
    dbg('decision', { kind: 'skip-whole-batch', sent, status: firstStatus });
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
    dbg('decision', { kind: 'skip-one' });
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

  // Divide & conquista
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

// ---------- Função principal: aplica política nominal vs sugerido e responde ----------
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
  // Se x-batch-size ≤ 0, usamos BATCH_SIZE/env ou 20.
  const defaultNominal = Math.max(1, Number(process.env.BATCH_SIZE || 20));

  const batchNominal =
    Number.isFinite(batchSize) && batchSize > 0 ? Number(batchSize) : defaultNominal;

  // Sugerido só vale se > 0
  const batchSugerido =
    Number.isFinite(batchSizeSugerido) && batchSizeSugerido > 0 ? Number(batchSizeSugerido) : 0;

  // Efetivo = sugerido (se ativo), senão nominal
  const batchEfetivo = batchSugerido > 0 ? batchSugerido : batchNominal;

  // DRY-RUN: apenas devolve o esqueleto (útil para validar headers/caminho)
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
    return respostaJson(200, {
      nextOffset: offsetInicial,
      suggestBatchSize: batchSugerido,
      done: true,
      summary
    });
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
    // Fatia conforme batch EFETIVO
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

    // Se houve erro fatal de schema e nenhum progresso, propaga 4xx do upstream
    const progressoFatia = (res.accepted || 0) + (res.skippedDuplicates || 0);
    if (res.fatalError && progressoFatia === 0) {
      const baseHeaders = {
        'Content-Type': 'application/json',
        'Retry-After': String(RETRY_AFTER_SECS),
        'x-batch-size': String(batchNominal),
        'x-suggest-batch-size': String(batchEfetivo),
      };
      return {
        statusCode: res.fatalError.statusCode || 400,
        headers: baseHeaders,
        body: res.fatalError.body || JSON.stringify({ error: 'Upstream error' }),
      };
    }

    // Consolida métricas
    totais.recordsInserted += res.inserted;
    totais.recordsUpdated  += res.updated;
    totais.recordsDeleted  += res.deleted;
    errosBatchesCount      += res.errosBatchesCountDelta;
    totalBatches           += Math.max(1, res.batchesUsed);
    skippedDuplicates      += res.skippedDuplicates;

    aceitosTotais += progressoFatia;
    indiceAceito   = (offsetInicial || 0) + aceitosTotais;

    // nextOffset de saída: se não houve progresso, mantém o offsetInicial
    const nextOffsetOut = (progressoFatia > 0) ? indiceAceito : (offsetInicial || 0);

    // Avança cursor interno desta invocação
    const temMais = fim < todos.length;
    dbg('loop', {
      indiceInicial: indice,
      fatia: fatia.length,
      progressoFatia,
      indiceAceito,
      nextOffsetOut,
      batchEfetivo
    });
    indice = SINGLE_BATCH_MODE ? fim : Math.max(indiceAceito, indice);

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

    // Política para x-suggest-batch-size de retorno
    let nextSuggest = batchSugerido; // default: manter como veio
    if (progressoFatia === 0) {
      nextSuggest = Math.max(1, Math.floor(batchEfetivo / 2)); // reduzir
    } else if (res.usedAdaptiveSplit) {
      // houve split interno → continuar pequeno por mais uma rodada
      nextSuggest = (batchSugerido > 0) ? batchSugerido : batchEfetivo;
    } else {
      // passou “limpo” → resetar sugestão
      nextSuggest = 0;
    }

    // Cabeçalhos sempre ecoados
    const baseHeaders = {
      'Content-Type': 'application/json',
      'x-batch-size': String(batchNominal),
      'x-suggest-batch-size': String(nextSuggest),
    };

    // SINGLE_BATCH_MODE: responde e encerra (1 lote por invocação)
    if (SINGLE_BATCH_MODE) {
      if (temMais) {
        return {
          statusCode: 206,
          headers: { ...baseHeaders, 'Retry-After': String(RETRY_AFTER_SECS) },
          body: JSON.stringify({
            nextOffset: nextOffsetOut,
            suggestBatchSize: nextSuggest,
            done: false,
            summary,
            hints: { suggestNextBatchSize: nextSuggest },
          }),
        };
      }
      // Concluído
      return {
        statusCode: 200,
        headers: baseHeaders,
        body: JSON.stringify({
          nextOffset: null,
          suggestBatchSize: 0,
          done: true,
          summary,
          hints: { suggestNextBatchSize: 0 },
        }),
      };
    }

    // (modo multi-lotes): orçamento estourado → 206
    if ((res.budgetLeft ?? 1) <= 0) {
      return {
        statusCode: 206,
        headers: { ...baseHeaders, 'Retry-After': String(RETRY_AFTER_SECS) },
        body: JSON.stringify({
          nextOffset: nextOffsetOut,
          suggestBatchSize: nextSuggest,
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
    body: JSON.stringify({
      nextOffset: null,
      suggestBatchSize: 0,
      done: true,
      summary,
      hints: { suggestNextBatchSize: 0 },
    }),
  };
}

module.exports = { executarEnvioDireto };

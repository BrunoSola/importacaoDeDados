// src/handler.js
// Lambda Importadora genérica: lê arquivo (xlsx/xls/csv) e envia cada linha
// - Modo integração: envia cada registro para a lambda-integracao-flowch (INTEGRATION_URL)
// - Modo direto: envia em lotes diretamente para um endpoint do Flowch (x-endpoint-url)
// Regras de negócio/validações ficam NA INTEGRAÇÃO.
//
// Headers principais:
//  - Authorization: <token cru do Flowch> (obrigatório em ambos os modos)
//  - endpoint: <nome do endpoint> (obrigatório no modo integração)
//  - x-endpoint-url: <URL completa do Flowch> (ativa o modo direto; substitui o endpoint)
//  - x-batch-size: <int> (opcional no modo direto; default 20)
//  - x-dry-run: true|false (simulação; não envia)
//  - x-preview: true|false (retorna primeiras linhas)
//  - x-limit: N (processa apenas N linhas)
//  - x-offset: N (retomada por índice 0-based; opcional)
//
// Corpo: arquivo (multipart, csv/xlsx) ou { base64, filename?, contentType? }
//
// Datas: strings em formatos dd/MM/yyyy, dd/MM/yyyy HH:mm, yyyy-MM-dd, yyyy-MM-dd HH:mm e
// yyyy-MM-dd[ T]HH:mm:ss são normalizadas para "yyyy-MM-dd HH:mm:ss".

const { httpJson } = require('./core/httpClient');
const { toLowerHeaders, readBodyBase64, parseFlags } = require('./utils/parseEvent');
const { parseFileToObjects, detectKind } = require('./utils/fileParser');

// Modo direto → envio por lotes
const { sendBatchesDirectToFlowch } = require('./utils/flowchDirectSender');
const { looksLikeNFe, transformNfeRows } = require('./transformers/nfe');

// ENV
const INTEGRATION_URL = process.env.INTEGRATION_URL; // URL (API GW/Lambda URL) da lambda-integracao-flowch
const DEFAULT_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 15000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 5)); // paralelismo (modo integração)
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || CONCURRENCY)); // tamanho do lote (modo integração)
const PREVIEW_LIMIT = 5;
const SAFE_MS = Math.max(500, Number(process.env.SAFE_REMAINING_MS || 4000)); // margem para 206

// util: sleep
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// helpers de métricas
function mean(nums) {
  if (!nums.length) return 0;
  return Math.round(nums.reduce((a, b) => a + b, 0) / nums.length);
}
function percentile(nums, p) {
  if (!nums.length) return 0;
  const arr = [...nums].sort((a, b) => a - b);
  const idx = Math.min(arr.length - 1, Math.max(0, Math.floor((p / 100) * arr.length)));
  return arr[idx];
}

function toNumberLocale(input, kind = 'float') {
  if (input == null || input === '') return input;
  if (typeof input === 'number') return input;
  let s = String(input).trim();
  // Heurística BR: se tem . e , e a última vírgula vem depois do último ponto → 1.234,56
  const lastComma = s.lastIndexOf(',');
  const lastDot = s.lastIndexOf('.');
  if (/[.,]/.test(s)) {
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(/,/g, '.');
    else s = s.replace(/,/g, '.');
  }
  const num = kind === 'int' ? parseInt(s, 10) : parseFloat(s);
  return Number.isFinite(num) ? num : input;
}

function coerceValue(v, type) {
  if (!type) return (typeof v === 'string' ? v.trim() : v);
  const t = String(type).toLowerCase();
  switch (t) {
    case 'int':
    case 'integer':  return toNumberLocale(v, 'int');
    case 'float':
    case 'number':
    case 'decimal':  return toNumberLocale(v, 'float');
    case 'bool':
    case 'boolean':  return normalizeBoolean(v);
    case 'string':   return v == null ? v : String(v).trim();
    case 'date':     return v == null ? v : String(v).trim(); // normaliza depois no sanitize
    default:         return (typeof v === 'string' ? v.trim() : v);
  }
}

function parseTypeMap(headers) {
  const map = {};
  const raw = headers['x-const-types'];
  if (raw) {
    try {
      if (typeof raw === 'string' && raw.trim().startsWith('{')) {
        const obj = JSON.parse(raw);
        for (const [k, t] of Object.entries(obj)) map[String(k).trim()] = String(t).toLowerCase();
      } else {
        String(raw).split(',').forEach(pair => {
          const [k, t] = pair.split(':').map(s => s.trim()).filter(Boolean);
          if (k && t) map[k] = t.toLowerCase();
        });
      }
    } catch {/* ignore */}
  }
  const addList = (h, type) => {
    const v = headers[h];
    if (!v) return;
    String(v).split(',').map(s => s.trim()).filter(Boolean).forEach(k => (map[k] = type));
  };
  addList('x-const-int',   'int');
  addList('x-const-float', 'float');
  addList('x-const-bool',  'bool');
  addList('x-const-string','string');
  return map;
}

function inferTypedKey(headerKey) {
  const m = String(headerKey)
    .match(/^x-const-(.+?)(?:[_.](int|integer|float|number|decimal|bool|boolean|string|date))$/i);
  return m ? { field: m[1], type: m[2].toLowerCase() } : null;
}

/**
 * Extração de constantes dos headers.
 * Compatível com chamadas antigas (1 argumento) e novas (2 argumentos).
 * - headersLower: headers normalizados em minúsculas (uso lógico)
 * - headersRaw: headers como chegaram do provedor (para tentar preservar o case do field)
 */
function extractConsts(arg1, arg2) {
  const headersLower = arg1 || {};
  const headersRaw   = arg2 || {};

  const consts = {};
  const types  = parseTypeMap(headersLower);

  // Índice lower → original (para recuperar case quando disponível)
  const rawIndex = {};
  if (headersRaw && typeof headersRaw === 'object') {
    for (const rk of Object.keys(headersRaw)) {
      rawIndex[String(rk).toLowerCase()] = rk;
    }
  }

  for (const [kLower, v] of Object.entries(headersLower || {})) {
    if (!kLower.startsWith('x-const-')) continue;

    // pular chaves reservadas
    if ([
      'x-const',
      'x-consts',
      'x-const-types',
      'x-const-int',
      'x-const-float',
      'x-const-bool',
      'x-const-string'
    ].includes(kLower)) continue;

    // tenta recuperar com o case original; se não houver, usa o lower
    const kOrig = rawIndex[kLower] || kLower;

    // Campo padrão = tudo após "x-const-"
    let field = kOrig.slice('x-const-'.length);

    // Tipo explícito via mapa (aceita exato e lower)
    let explicitType = types[field] || types[field?.toLowerCase()];

    // Sufixo tipado no próprio header (usa kOrig para preservar case do field)
    const typed = inferTypedKey(kOrig);
    if (typed) {
      field = typed.field;                  // mantém a grafia do caller
      explicitType = typed.type || explicitType;
    }

    if (v !== undefined && v !== null && String(v).trim() !== '') {
      consts[field] = coerceValue(v, explicitType);
    }
  }

  // JSON opcional em x-const / x-consts (mantém case das chaves do JSON)
  const jsonConsts = headersLower['x-const'] || headersLower['x-consts'];
  if (jsonConsts) {
    try {
      const parsed = typeof jsonConsts === 'string' ? JSON.parse(jsonConsts) : jsonConsts;
      if (parsed && typeof parsed === 'object') {
        for (const [k, v] of Object.entries(parsed)) {
          consts[k] = types[k] ? coerceValue(v, types[k]) : v;
        }
      }
    } catch { /* JSON inválido: ignorar */ }
  }

  return consts;
}

function applyConsts(obj, consts, override = false) {
  const out = { ...obj };
  for (const [k, v] of Object.entries(consts || {})) {
    const cur = out[k];
    const isEmpty = cur === undefined || cur === null || String(cur).trim() === '';
    if (override || isEmpty) {
      out[k] = typeof v === 'string' ? v.trim() : v;
    }
  }
  return out;
}


// --- Sniff helpers (inferir tipo/nome quando vier JSON/base64) ---
function isLikelyText(buf) {
  const head = buf.slice(0, Math.min(buf.length, 4096));
  const hasNulls = head.includes(0x00);
  const hasNewline = head.includes(0x0A) || head.includes(0x0D);
  return !hasNulls && hasNewline;
}

function sniffContentType(buf) {
  if (buf.length >= 2 && buf[0] === 0x50 && buf[1] === 0x4B) return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  const head = buf.slice(0, 64).toString('utf8').trim();
  if (head.startsWith('<')) return 'application/xml';
  if (isLikelyText(buf)) return 'text/csv';
  return 'application/octet-stream';
}

function defaultFilenameByType(ct) {
  if ((ct || '').includes('spreadsheetml')) return 'upload.xlsx';
  if ((ct || '').startsWith('text/')) return 'upload.csv';
  return 'upload.bin';
}

// >>> Normalização de data/hora para yyyy-MM-dd HH:mm:ss
function normalizeDateTime(value) {
  if (value == null || value === '') return value;
  const str = String(value).trim();

  // dd/MM/yyyy HH:mm:ss
  let m = str.match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2}):(\d{2})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]} ${m[4]}:${m[5]}:${m[6]}`;

  // dd/MM/yyyy
  m = str.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]} 00:00:00`;

  // dd/MM/yyyy HH:mm
  m = str.match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]} ${m[4]}:${m[5]}:00`;

  // yyyy-MM-dd
  m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]} 00:00:00`;

  // yyyy-MM-dd HH:mm
  m = str.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:00`;

  // yyyy-MM-dd[ T]HH:mm:ss[.SSS][Z|±HH:mm]
  m = str.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]}`;

  // yyyy-MM-dd[ T]HH:mm
  m = str.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:00`;

  return str; // não reconhecido → mantém
}


// Aplica a normalização em todas as colunas string do registro
function normalizeRecordDates(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    out[k] = (typeof v === 'string') ? normalizeDateTime(v) : v;
  }
  return out;
}

function normalizeBoolean(value) {
  if (value == null) return value;

  if (typeof value === 'boolean') return value;

  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (['true','1','yes','y','sim','s'].includes(v)) return true;
    if (['false','0','no','n','nao','não'].includes(v)) return false;
  }

  if (typeof value === 'number') {
    if (value === 1) return true;
    if (value === 0) return false;
  }

  return value; // não reconhecido → mantém original
}


async function handler(event, context) {
  const started = Date.now();

  try {
    // 1) Headers
    const headersRaw   = (event && event.headers) ? event.headers : {};
    const headersLower = toLowerHeaders(headersRaw);

    // Alias para retrocompatibilidade: o restante do arquivo usa "headers"
    const headers = headersLower;

    const endpoint = headers['endpoint']; // usado no modo integração
    const authorization = headers['authorization'];

    const { dryRun, preview } = parseFlags(headers);
    const stopOnError = String(headers['x-stop-on-error'] || '').toLowerCase() === 'true';
    const logProgress = String(headers['x-log-progress'] || '').toLowerCase() === 'true';
    const consts = extractConsts(headersLower, headersRaw); // usa raw para preservar case do field
    const overrideConsts = String(headers['x-override-consts'] || '').toLowerCase() === 'true';

    // retomada opcional
    const startOffset = Math.max(0, parseInt(headers['x-offset'] || '0', 10));
    const uploadId = headers['x-upload-id'] || null;
    const fileHash = headers['x-file-sha256'] || null;

    // modo direto — URL completa do Flowch e batch-size custom
    const directEndpointUrl = headers['x-endpoint-url'] || '';
    const directBatchSize  = Number(headers['x-batch-size'] || 100);
    const useDirect = !!directEndpointUrl;

    // 2) Validações iniciais
    if (!authorization) {
      return json(400, { error: 'Header "Authorization" é obrigatório.' });
    }

    if (useDirect) {
      if (!directEndpointUrl) {
        return json(400, { error: 'x-endpoint-url inválido.' });
      }
    } else {
      if (!endpoint) {
        return json(400, { error: 'Header "endpoint" é obrigatório (ou use x-endpoint-url).' });
      }
      if (!INTEGRATION_URL) {
        return json(500, { error: 'INTEGRATION_URL não configurada (env var).' });
      }
    }

    // 3) Arquivo (multipart/base64) → Buffer
    const file = readBodyBase64(event);
    let { contentType, filename, buffer } = file;
    if (!buffer?.length) return json(400, { error: 'Arquivo ausente no body (base64/multipart) ou vazio.' });

    // Inferir quando vier JSON/base64 sem dados do arquivo
    const inferredType = sniffContentType(buffer);
    if (!contentType || contentType === 'application/json') {
      contentType = inferredType;
    }
    if (!filename) {
      filename = defaultFilenameByType(contentType);
    }

    // Log de diagnóstico
    console.log('debug-upload', {
      contentType,
      filename,
      size: buffer?.length,
      headHex: buffer?.slice(0, 8)?.toString('hex')
    });

    // 4) Parse do arquivo → array de objetos (1ª linha = cabeçalho)
    let rows;
    try {
      rows = await parseFileToObjects({ buffer, contentType, filename, headers: headersLower });
      console.log('file-kind', { decidedKind: detectKind({ contentType, filename }) });
    } catch (e) {
      const status = e.statusCode || 400;
      return json(status, { error: e.message || 'Falha ao ler arquivo.' });
    }

    if (!rows.length) {
      return json(400, { error: 'Sem linhas de dados (verifique o cabeçalho na primeira linha).' });
    }

    // Limitar via header (ex.: x-limit: 1)
    const limit = parseInt(headers['x-limit'] || '0', 10);
    if (limit > 0) {
      rows = rows.slice(0, limit);
    }
    if (looksLikeNFe({ contentType, filename, buffer })) {
      rows = transformNfeRows(rows);
    }

    // 5) Pré-visualização
    const previewRows = preview
      ? rows
          .slice(0, PREVIEW_LIMIT)
          .map(r => applyConsts(r, consts, overrideConsts))
          .map(sanitizeFlatRow)
      : undefined;

    // 6) MODO DIRETO (envia por lotes diretamente ao Flowch)
    if (useDirect) {
      if (dryRun) {
        const duration = Date.now() - started;
        return json(200, {
          nextOffset: startOffset,
          done: true,
          summary: {
            modo: 'direto-flowch',
            endpointUrl: directEndpointUrl,
            linhasLidas: rows.length,
            enviadasAprox: 0,
            errosBatches: 0,
            duracaoMs: duration,
            dryRun: true,
            preview: !!preview,
            batchSize: Number.isFinite(directBatchSize) && directBatchSize > 0 ? directBatchSize : 20,
            totalBatches: 0,
            uploadId, fileHash,
            size: 0,
            recordsInserted: 0,
            recordsUpdated: 0,
            recordsDeleted: 0
          }
        });
      }

      // normaliza e aplica consts antes de fatiar por offset
      const payloadRecordsAll = rows
        .map(r => applyConsts(r, consts, overrideConsts))
        .map(sanitizeFlatRow);

      let i = Math.min(startOffset, payloadRecordsAll.length);
      const resultsAgg = [];
      let totals = { size: 0, recordsInserted: 0, recordsUpdated: 0, recordsDeleted: 0, errors: [] };

      while (i < payloadRecordsAll.length) {
        const end = Math.min(i + (Number.isFinite(directBatchSize) && directBatchSize > 0 ? directBatchSize : 20), payloadRecordsAll.length);
        const payloadSlice = payloadRecordsAll.slice(i, end);

        const agg = await sendBatchesDirectToFlowch({
          endpointUrl: directEndpointUrl,
          token: authorization, // token cru (o sender prefixa "integration ")
          records: payloadSlice,
          batchSize: Number.isFinite(directBatchSize) && directBatchSize > 0 ? directBatchSize : 20,
          timeoutMs: DEFAULT_TIMEOUT_MS,
          method: 'POST',
        });

        // agrega resultados
        resultsAgg.push(...agg.results);
        for (const r of agg.results) {
          const b = r.body || {};
          totals.size += (r.size || 0);
          totals.recordsInserted += (b.recordsInserted || 0);
          totals.recordsUpdated  += (b.recordsUpdated  || 0);
          totals.recordsDeleted  += (b.recordsDeleted  || 0);
          if (Array.isArray(b.errors)) totals.errors.push(...b.errors);
        }

        i = end;

        // corte por tempo
        const elapsed = Date.now() - started;
        if (elapsed >= (25000 - SAFE_MS)) {
          const duration = Date.now() - started;
          const acceptedApprox = resultsAgg.reduce((n, r) =>
            (r.statusCode >= 200 && r.statusCode < 300) ? n + r.size : n, 0);

          return {
            statusCode: 206,
            headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
            body: JSON.stringify({
              nextOffset: i,
              done: false,
              summary: {
                modo: 'direto-flowch',
                endpointUrl: directEndpointUrl,
                linhasLidas: rows.length,
                enviadasAprox: acceptedApprox,
                errosBatches: resultsAgg.filter(r => !(r.statusCode >= 200 && r.statusCode < 300)).length,
                duracaoMs: duration,
                dryRun: false,
                preview: !!preview,
                batchSize: Number.isFinite(directBatchSize) && directBatchSize > 0 ? directBatchSize : 20,
                totalBatches: resultsAgg.length,
                uploadId, fileHash,
                size: totals.size,
                recordsInserted: totals.recordsInserted,
                recordsUpdated: totals.recordsUpdated,
                recordsDeleted: totals.recordsDeleted
              }
            })
          };
        }
      }

      // terminou tudo
      const duration = Date.now() - started;
      const acceptedApprox = resultsAgg.reduce((n, r) =>
        (r.statusCode >= 200 && r.statusCode < 300) ? n + r.size : n, 0);

      // coletar amostras de erro (até 5)
      const errorSamples = [];
      for (const r of resultsAgg) {
        if (!(r.statusCode >= 200 && r.statusCode < 300) && errorSamples.length < 5) {
          const bodyStr = typeof r.body === 'string' ? r.body : JSON.stringify(r.body);
          errorSamples.push({
            statusCode: r.statusCode,
            snippet: String(bodyStr).slice(0, 400)
          });
        }
      }

      return json(200, {
        nextOffset: null,
        done: true,
        summary: {
          modo: 'direto-flowch',
          endpointUrl: directEndpointUrl,
          linhasLidas: rows.length,
          enviadasAprox: acceptedApprox,
          errosBatches: resultsAgg.filter(r => !(r.statusCode >= 200 && r.statusCode < 300)).length,
          duracaoMs: duration,
          dryRun: false,
          preview: !!preview,
          batchSize: Number.isFinite(directBatchSize) && directBatchSize > 0 ? directBatchSize : 20,
          totalBatches: resultsAgg.length,
          uploadId, fileHash,
          size: totals.size,
          recordsInserted: totals.recordsInserted,
          recordsUpdated: totals.recordsUpdated,
          recordsDeleted: totals.recordsDeleted,
          errorSamples
        }
      });
    }

    // 7) MODO INTEGRAÇÃO (fluxo original)
    let sent = 0, already = 0, errors = 0;
    const results = [];
    const errorSamples = [];

    // Quebra em lotes iniciando do startOffset
    for (let offset = Math.min(startOffset, rows.length); offset < rows.length; offset += BATCH_SIZE) {
      const batch = rows.slice(offset, offset + BATCH_SIZE);

      // Executa o batch com paralelismo máximo = CONCURRENCY
      const batchResults = await mapWithConcurrency(batch, CONCURRENCY, async (row, idxInBatch) => {
        const globalRowIndex = offset + idxInBatch; // 0-based
        const linhaExcel = globalRowIndex + 2; // +2 pq 1 é header e somamos 1 para 1-based

        let payload = applyConsts(row, consts, overrideConsts);
        payload = sanitizeFlatRow(payload);

        if (dryRun) {
          sent++;
          return {
            linha: linhaExcel,
            endpoint,
            status: 'ENVIADO (DRY-RUN)',
            mensagem: 'Simulação – não enviado.',
            perfMs: 0
          };
        }

        try {
          const t0 = Date.now();
          const resp = await httpJson(
            INTEGRATION_URL,
            'POST',
            {
              'Content-Type': 'application/json',
              'Authorization': authorization, // token cru (prefixo é adicionado pela integração)
              'endpoint': endpoint
            },
            JSON.stringify(payload),
            DEFAULT_TIMEOUT_MS
          );
          const dt = Date.now() - t0;

          let body = {};
          try { body = JSON.parse(resp.body || '{}'); } catch { body = { raw: resp.body }; }

          if (resp.statusCode === 200 && body?.message === 'ALREADY_EXISTS') {
            already++;
            return {
              linha: linhaExcel,
              endpoint,
              status: 'ALREADY_EXISTS',
              mensagem: 'Registro já existia no destino.',
              upstreamStatus: resp.statusCode,
              perfMs: dt
            };
          }

          if (resp.statusCode >= 200 && resp.statusCode < 300) {
            sent++;
            return {
              linha: linhaExcel,
              endpoint,
              status: 'ENVIADO',
              mensagem: 'OK',
              upstreamStatus: resp.statusCode,
              perfMs: dt
            };
          }

          // erro de aplicação/upstream
          errors++;
          const msg = body?.error || (typeof body === 'string' ? body : JSON.stringify(body));
          const erroObj = {
            linha: linhaExcel,
            endpoint,
            status: 'ERRO',
            mensagem: msg,
            upstreamStatus: resp.statusCode,
            upstreamBody: body,
            perfMs: dt
          };
          if (errorSamples.length < 5) errorSamples.push(erroObj);
          if (stopOnError) throw new Error(`[STOP_ON_ERROR] ${msg}`);
          return erroObj;

        } catch (e) {
          errors++;
          const erroObj = {
            linha: linhaExcel,
            endpoint,
            status: 'ERRO',
            mensagem: e.message || 'Falha ao enviar'
          };
          if (errorSamples.length < 5) errorSamples.push(erroObj);
          if (stopOnError) throw e;
          return erroObj;
        }
      });

      results.push(...batchResults);

      if (logProgress) {
        const dts = batchResults
          .map(r => (typeof r?.perfMs === 'number' ? r.perfMs : null))
          .filter(v => v !== null && v >= 0);

        console.log(JSON.stringify({
          progress: {
            processed: Math.min(offset + batch.length, rows.length),
            total: rows.length,
            sent,
            already,
            errors
          },
          batchPerf: {
            count: dts.length,
            avgMs: mean(dts),
            p95Ms: percentile(dts, 95)
          }
        }));
      }

      const sleepMs = Number(process.env.BATCH_SLEEP_MS || 0);
      if (sleepMs > 0 && offset + BATCH_SIZE < rows.length) {
        await wait(sleepMs);
      }

      // corte por tempo para retorno parcial 206
      if (context && typeof context.getRemainingTimeInMillis === 'function' &&
          context.getRemainingTimeInMillis() <= SAFE_MS) {
        const duration = Date.now() - started;
        const nextOffset = Math.min(offset + batch.length, rows.length);
        return {
          statusCode: 206,
          headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
          body: JSON.stringify({
            nextOffset,
            done: false,
            summary: {
              arquivo: filename,
              contentType,
              linhasLidas: rows.length,
              enviadas: sent,
              jaExistiam: already,
              erros: errors,
              duracaoMs: duration,
              dryRun: !!dryRun,
              preview: !!preview,
              paralelismo: CONCURRENCY,
              batchSize: BATCH_SIZE,
              uploadId, fileHash
            }
          })
        };
      }

      if (stopOnError && errors > 0 && !dryRun) break;
    }

    const duration = Date.now() - started;

    return json(200, {
      nextOffset: null,
      done: true,
      summary: {
        arquivo: filename,
        contentType,
        linhasLidas: rows.length,
        enviadas: sent,
        jaExistiam: already,
        erros: errors,
        duracaoMs: duration,
        dryRun: !!dryRun,
        preview: !!preview,
        paralelismo: CONCURRENCY,
        batchSize: BATCH_SIZE,
        uploadId, fileHash
      }
    });

  } catch (err) {
    return json(err.statusCode || 500, { error: err.message || 'Erro inesperado' });
  }
}

/**
 * Executa um array de itens com paralelismo limitado (sem libs externas).
 * @param {Array<any>} items
 * @param {number} concurrency
 * @param {(item:any, index:number)=>Promise<any>} worker
 * @returns {Promise<Array<any>>}
 */
async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function run() {
    while (true) {
      const i = nextIndex++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  }

  const runners = [];
  for (let k = 0; k < Math.min(concurrency, items.length); k++) {
    runners.push(run());
  }
  await Promise.all(runners);
  return results;
}

// Aparar strings e normalizar datas
function sanitizeFlatRow(obj) {
  const trimmed = {};
  for (const [k, v] of Object.entries(obj || {})) {
    let val = (typeof v === 'string') ? v.trim() : (v ?? '');

    // aplica normalizações
    if (typeof val === 'string') {
      val = normalizeDateTime(val);   // datas
      val = normalizeBoolean(val);    // booleans
    } else {
      val = normalizeBoolean(val);
    }

    trimmed[k] = val;
  }
  return trimmed;
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  };
}

module.exports = { handler };

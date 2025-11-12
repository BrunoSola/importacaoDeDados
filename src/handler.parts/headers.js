// src/handler.parts/headers.js
//
// Objetivo:
// - Centralizar toda a leitura/normalização de headers e variáveis de ambiente.
// - Retornar um objeto de configuração "cfg" com as seções:
//   - cfg.headers / cfg.headersRaw: headers minúsculos e originais
//   - cfg.flags: { preview, dryRun, logProgress, stopOnError }
//   - cfg.general: { limit, defaultTimeoutMs, apigwSoftTimeoutMs, safeRemainingMs, concurrency, batchSize, iniciouEm }
//   - cfg.direct: { useDirect, endpointUrl, batchSize }
//   - cfg.ids: { uploadId, fileHash }
//   - cfg.integration: { endpoint, integrationUrl, authorization }
//   - cfg.error: caso falte algo essencial, padroniza { status, msg }
//
// Observação:
// - Não executa envio e nem lê arquivo; só prepara o "contrato" de operação.

const { toLowerHeaders } = require('../utils/parseEvent');

function num(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function truthy(v) {
  return String(v || '').toLowerCase() === 'true';
}

function buildConfigFromEvent(event, context) {
  const headersRaw = (event && event.headers) ? event.headers : {};
  const headers = toLowerHeaders(headersRaw);

  const iniciouEm = Date.now();

  const authorization = headers['authorization'] || '';
  const endpoint = headers['endpoint'] || '';
  const endpointUrl = headers['x-endpoint-url'] || '';

  const useDirect = !!endpointUrl;

  // Flags de comportamento:
  const flags = {
    preview: truthy(headers['x-preview']),
    dryRun: truthy(headers['x-dry-run']),
    logProgress: truthy(headers['x-log-progress']),
    stopOnError: truthy(headers['x-stop-on-error']),
  };

  // Controle geral:
  const general = {
    limit: Math.max(0, parseInt(headers['x-limit'] || '0', 10)),
    defaultTimeoutMs: num(process.env.HTTP_TIMEOUT_MS, 15000),
    apigwSoftTimeoutMs: num(process.env.APIGW_SOFT_TIMEOUT_MS, 29000),
    safeRemainingMs: Math.max(500, num(process.env.SAFE_REMAINING_MS, 4000)),
    concurrency: Math.max(1, num(process.env.CONCURRENCY, 5)),
    batchSize: Math.max(1, num(process.env.BATCH_SIZE, 5)),
    iniciouEm,
  };

  // Direto:
  const direct = {
    useDirect,
    endpointUrl,
    batchSize: Math.max(1, num(headers['x-batch-size'], 100)),
  };

  // Identificadores auxiliares:
  const ids = {
    uploadId: headers['x-upload-id'] || null,
    fileHash: headers['x-file-sha256'] || null,
  };

  // Integração:
  const integration = {
    endpoint,
    integrationUrl: process.env.INTEGRATION_URL || '',
    authorization,
  };

  // Validações mínimas
  if (!authorization) {
    return { error: { status: 400, msg: 'Header "Authorization" é obrigatório.' }, headers, headersRaw };
  }
  if (useDirect) {
    if (!endpointUrl) {
      return { error: { status: 400, msg: 'x-endpoint-url inválido.' }, headers, headersRaw };
    }
  } else {
    if (!endpoint) {
      return { error: { status: 400, msg: 'Header "endpoint" é obrigatório (ou use x-endpoint-url).' }, headers, headersRaw };
    }
    if (!integration.integrationUrl) {
      return { error: { status: 500, msg: 'INTEGRATION_URL não configurada (env var).' }, headers, headersRaw };
    }
  }

  return {
    headers,
    headersRaw,
    flags,
    general,
    direct,
    ids,
    integration,
    context,
  };
}

module.exports = { buildConfigFromEvent };

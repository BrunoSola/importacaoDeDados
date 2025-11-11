// src/handler.parts/headers.js
const { toLowerHeaders, parseFlags } = require('../utils/parseEvent');

const num = (v, fallback) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

function buildConfigFromEvent(event, context) {
  const env = {
    INTEGRATION_URL: process.env.INTEGRATION_URL,
    DEFAULT_TIMEOUT_MS: num(process.env.HTTP_TIMEOUT_MS, 15000),
    APIGW_SOFT_TIMEOUT_MS: num(process.env.APIGW_SOFT_TIMEOUT_MS, 29000),
    CONCURRENCY: Math.max(1, num(process.env.CONCURRENCY, 5)),
    BATCH_SIZE_ENV: num(process.env.BATCH_SIZE, NaN),
    SAFE_MS: Math.max(500, num(process.env.SAFE_REMAINING_MS, 4000)),
  };

  const headersRaw = (event && event.headers) ? event.headers : {};
  const headers = toLowerHeaders(headersRaw);

  const { dryRun, preview } = parseFlags(headers);
  const endpoint = headers['endpoint'];
  const authorization = headers['authorization'];
  const startOffset = Math.max(0, parseInt(headers['x-offset'] || '0', 10));
  const uploadId = headers['x-upload-id'] || null;
  const fileHash = headers['x-file-sha256'] || null;

  const directEndpointUrl = headers['x-endpoint-url'] || '';
  const directBatchSize = num(headers['x-batch-size'], 100);
  const useDirect = !!directEndpointUrl;

  const stopOnError = String(headers['x-stop-on-error'] || '').toLowerCase() === 'true';
  const logProgress = String(headers['x-log-progress'] || '').toLowerCase() === 'true';
  const limit = parseInt(headers['x-limit'] || '0', 10);

  // Batch size efetivo: se não configurar, cai para CONCURRENCY
  const batchSizeIntegracao = Math.max(1, Number.isFinite(env.BATCH_SIZE_ENV) ? env.BATCH_SIZE_ENV : env.CONCURRENCY);

  // Validações mínimas
  if (!authorization) {
    return { error: { status: 400, msg: 'Header "Authorization" é obrigatório.' } };
  }
  if (useDirect) {
    if (!directEndpointUrl) {
      return { error: { status: 400, msg: 'x-endpoint-url inválido.' } };
    }
  } else {
    if (!endpoint) {
      return { error: { status: 400, msg: 'Header "endpoint" é obrigatório (ou use x-endpoint-url).' } };
    }
    if (!env.INTEGRATION_URL) {
      return { error: { status: 500, msg: 'INTEGRATION_URL não configurada (env var).' } };
    }
  }

  return {
    env,
    headersRaw,
    headers,
    flags: { dryRun, preview, stopOnError, logProgress },
    ids: { uploadId, fileHash },
    offsets: { startOffset },
    direct: { useDirect, directEndpointUrl, directBatchSize },
    integracao: { endpoint, batchSizeIntegracao },
    general: { limit },
    contexto: { event, context, iniciouEm: Date.now() },
    auth: { authorization },
  };
}

module.exports = { buildConfigFromEvent };
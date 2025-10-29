// src/handler.js
// Lambda Importadora genérica: lê arquivo (xlsx/xls/csv) e envia cada linha
// - Modo integração: envia cada registro para a lambda-integracao-flowch (INTEGRATION_URL)
// - Modo direto: envia em lotes diretamente para um endpoint do Flowch (x-endpoint-url)
// Regras de negócio/validações ficam NA INTEGRAÇÃO.

const { httpJson } = require('./core/httpClient');
const { toLowerHeaders, parseFlags } = require('./utils/parseEvent');
const { extrairConstantes, aplicarConstantes } = require('./utils/constantes');
const { limparRegistroPlano } = require('./utils/registros');
const { respostaJson } = require('./utils/httpResponse');
const { prepararArquivo } = require('./services/arquivoService');
const { executarEnvioDireto } = require('./services/envioDiretoService');
const { executarEnvioIntegracao } = require('./services/envioIntegracaoService');

const INTEGRATION_URL = process.env.INTEGRATION_URL; // URL da lambda-integracao-flowch
const DEFAULT_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || 15000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 5));
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || CONCURRENCY));
const PREVIEW_LIMIT = 5;
const SAFE_MS = Math.max(500, Number(process.env.SAFE_REMAINING_MS || 4000));

async function handler(event, context) {
  const iniciouEm = Date.now();

  try {
    const headersRaw = (event && event.headers) ? event.headers : {};
    const headersLower = toLowerHeaders(headersRaw);

    const headers = headersLower;
    const endpoint = headers['endpoint'];
    const authorization = headers['authorization'];

    const { dryRun, preview } = parseFlags(headers);
    const stopOnError = String(headers['x-stop-on-error'] || '').toLowerCase() === 'true';
    const logProgress = String(headers['x-log-progress'] || '').toLowerCase() === 'true';
    const consts = extrairConstantes(headersLower, headersRaw);
    const overrideConsts = String(headers['x-override-consts'] || '').toLowerCase() === 'true';

    const startOffset = Math.max(0, parseInt(headers['x-offset'] || '0', 10));
    const uploadId = headers['x-upload-id'] || null;
    const fileHash = headers['x-file-sha256'] || null;

    const directEndpointUrl = headers['x-endpoint-url'] || '';
    const directBatchSize = Number(headers['x-batch-size'] || 100);
    const useDirect = !!directEndpointUrl;

    if (!authorization) {
      return respostaJson(400, { error: 'Header "Authorization" é obrigatório.' });
    }

    if (useDirect) {
      if (!directEndpointUrl) {
        return respostaJson(400, { error: 'x-endpoint-url inválido.' });
      }
    } else {
      if (!endpoint) {
        return respostaJson(400, { error: 'Header "endpoint" é obrigatório (ou use x-endpoint-url).' });
      }
      if (!INTEGRATION_URL) {
        return respostaJson(500, { error: 'INTEGRATION_URL não configurada (env var).' });
      }
    }

    const limit = parseInt(headers['x-limit'] || '0', 10);

    const preparoArquivo = await prepararArquivo({
      event,
      headers: headersLower,
      gerarPreview: preview,
      limitePreview: PREVIEW_LIMIT,
      limitarLinhas: limit > 0 ? limit : undefined,
      formatarPreview: (registros) => registros
        .map((registro) => aplicarConstantes(registro, consts, overrideConsts))
        .map(limparRegistroPlano),
    });

    const linhasArquivo = preparoArquivo.linhas;
    const { filename, contentType } = preparoArquivo.arquivo;

    if (useDirect) {
      const registrosProcessados = linhasArquivo
        .map((registro) => aplicarConstantes(registro, consts, overrideConsts))
        .map(limparRegistroPlano);

      return executarEnvioDireto({
        registros: registrosProcessados,
        offsetInicial: startOffset,
        endpointUrl: directEndpointUrl,
        token: authorization,
        batchSize: directBatchSize,
        timeoutMs: DEFAULT_TIMEOUT_MS,
        iniciouEm,
        previewAtivo: preview,
        uploadId,
        fileHash,
        margemSegurancaMs: SAFE_MS,
        totalLinhas: linhasArquivo.length,
        dryRun,
      });
    }

    return executarEnvioIntegracao({
      linhas: linhasArquivo,
      startOffset,
      batchSize: BATCH_SIZE,
      concurrency: CONCURRENCY,
      consts,
      overrideConsts,
      dryRun,
      endpoint,
      authorization,
      integrationUrl: INTEGRATION_URL,
      timeoutMs: DEFAULT_TIMEOUT_MS,
      logProgress,
      stopOnError,
      contextoLambda: context,
      margemSegurancaMs: SAFE_MS,
      iniciouEm,
      previewAtivo: preview,
      resumoArquivo: {
        filename,
        contentType,
        uploadId,
        fileHash,
      },
      httpJson,
      aplicarConstantes,
      sanitizarRegistro: limparRegistroPlano,
    });
  } catch (err) {
    return respostaJson(err.statusCode || 500, { error: err.message || 'Erro inesperado' });
  }
}

module.exports = { handler };

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
const APIGW_SOFT_TIMEOUT_MS = Number(process.env.APIGW_SOFT_TIMEOUT_MS || 29000);
const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 5));
const BATCH_SIZE = Math.max(1, Number(process.env.BATCH_SIZE || CONCURRENCY));
const PREVIEW_LIMIT = 5;
const SAFE_MS = Math.max(500, Number(process.env.SAFE_REMAINING_MS || 4000));

async function handler(event, context) {
  if (context && typeof context.callbackWaitsForEmptyEventLoop === 'boolean') {
    context.callbackWaitsForEmptyEventLoop = false;
  }
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

    let usouTemplate =
    !!(headers['x-template-json-b64'] ||
      headers['x-template-json'] ||
      headers['x-template-json-url'] ||
      (event && (event.template_b64 || event.template || event.payloadTemplate)));

    const preparoArquivo = await prepararArquivo({
      event,
      headers: headersLower,
      gerarPreview: preview,
      limitePreview: PREVIEW_LIMIT,
      limitarLinhas: limit > 0 ? limit : undefined,
      formatarPreview: (registros) => {
        const arr = Array.isArray(registros) ? registros : (registros ? [registros] : []);
        const depoisConst = arr.map((registro) => aplicarConstantes(registro, consts, overrideConsts));
        return usouTemplate ? depoisConst.map(limparRegistroPlano) : depoisConst;
      },
    });

    const linhasArquivoRaw = preparoArquivo?.linhas ?? [];
    const linhasArquivo = Array.isArray(linhasArquivoRaw) ? linhasArquivoRaw : [linhasArquivoRaw];
    const { filename, contentType } = preparoArquivo.arquivo;

    // ===== Template via Header/Body =====
    // Fontes possíveis (em ordem de prioridade):
    // 1) Header: x-template-json-b64   -> JSON em Base64 (sem quebras de linha)
    // 2) Header: x-template-json       -> JSON puro (string)
    // 3) Header: x-template-json-url   -> JSON URL-encoded (encodeURIComponent)
    // 4) Body:   event.template_b64    -> JSON em Base64 (string)
    // 5) Body:   event.template        -> objeto OU string JSON
    // 6) Body:   event.payloadTemplate -> objeto OU string JSON
    // Opções (headers):
    // - x-template-single: 'true' | 'false' (default false)
    // - x-template-remove-empty: 'true' | 'false' (default true)
    // - x-template-auto-map: 'true' | 'false' (default true)
    let linhasParaEnviar = linhasArquivo;

    const tplB64Hdr = headers['x-template-json-b64'];
    const tplJsonHdr = headers['x-template-json'];
    const tplJsonUrlHdr = headers['x-template-json-url'];
    const tplBodyB64 = event.template_b64;
    const tplBodyRaw = event.template ?? event.payloadTemplate;

    const hasTemplate =
      !!tplB64Hdr || !!tplJsonHdr || !!tplJsonUrlHdr || !!tplBodyB64 || tplBodyRaw != null;

    if (hasTemplate) {
      try {
        const { construirPayload } = require('./transformers/templatePayload');

        let template;

        if (tplB64Hdr) {
          const jsonStr = Buffer.from(String(tplB64Hdr), 'base64').toString('utf8');
          template = JSON.parse(jsonStr);
        } else if (tplJsonHdr) {
          template = JSON.parse(String(tplJsonHdr));
        } else if (tplJsonUrlHdr) {
          template = JSON.parse(decodeURIComponent(String(tplJsonUrlHdr)));
        } else if (tplBodyB64) {
          const jsonStr = Buffer.from(String(tplBodyB64), 'base64').toString('utf8');
          template = JSON.parse(jsonStr);
        } else if (typeof tplBodyRaw === 'string') {
          template = JSON.parse(tplBodyRaw);
        } else if (tplBodyRaw && typeof tplBodyRaw === 'object') {
          template = tplBodyRaw;
        } else {
          throw new Error('Template não encontrado em header/body');
        }

        // Se vier array, usar o primeiro elemento como modelo
        if (Array.isArray(template)) {
          if (template.length === 0) throw new Error('Template array vazio');
          console.warn('[template] recebido como array — usando o primeiro item.');
          template = template[0];
        }

        usouTemplate = true;

        const single = String(headers['x-template-single'] || '').toLowerCase() === 'true';
        const removeEmpty = String(headers['x-template-remove-empty'] || 'true').toLowerCase() !== 'false';
        const autoMapSameNames = String(headers['x-template-auto-map'] || 'true').toLowerCase() !== 'false';

        const built = construirPayload(linhasArquivo, template, { removeEmpty, single, autoMapSameNames });
        linhasParaEnviar = single ? (built ? [built] : []) : built;

        console.log(`[template] aplicado: fonte=${tplB64Hdr?'hdr_b64':tplJsonHdr?'hdr_json':tplJsonUrlHdr?'hdr_url':tplBodyB64?'body_b64':'body'}, single=${single}, removeEmpty=${removeEmpty}, autoMap=${autoMapSameNames}, linhas=${linhasParaEnviar.length}`);
      } catch (e) {
        console.error('[template] erro ao processar', e?.message);
        return respostaJson(400, { error: `Template inválido: ${e.message}` });
      }
    }



    if (useDirect) {
      const base = linhasParaEnviar.map((r) => aplicarConstantes(r, consts, overrideConsts));
      const registrosProcessados = usouTemplate ? base.map(limparRegistroPlano) : base;     
      console.log("Vai chamar o envio Direto");
      console.log('[debug] qtd linhasParaEnviar:', Array.isArray(linhasParaEnviar) ? linhasParaEnviar.length : 0);
      console.log('[debug] amostra registro 0:', JSON.stringify(registrosProcessados[0] || null));

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
        totalLinhas: linhasParaEnviar.length,
        dryRun,
        contextoLambda: context,
        apigwSoftTimeoutMs: APIGW_SOFT_TIMEOUT_MS,
      });
    }

    return executarEnvioIntegracao({
      linhas: linhasParaEnviar,
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

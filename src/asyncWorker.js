// src/asyncWorker.js - worker assíncrono da importação
const { buildConfigFromEvent } = require('./handler.parts/headers');
const { prepararArquivo } = require('./services/arquivoService');
const { applyTemplateIfAny, applyConstantesAndSanitize } = require('./handler.parts/template');
const { buildPlanoDireto, buildPlanoIntegracao } = require('./handler.parts/plan');
const { executarEnvioDireto } = require('./services/envioDiretoService');
const { executarEnvioIntegracao } = require('./services/envioIntegracaoService');
const { httpJson } = require('./core/httpClient'); // pra callback

exports.handler = async (event, context) => {
  const { correlationId, callbackUrl, originalEvent } = event;
  console.log('asyncWorker start', { correlationId });

  let totalLinhas = 0;
  let enviadosOk = 0;
  let enviadosErro = 0;
  let statusFinal = 'COMPLETED';
  let erroGeral = null;

  try {
    // 1) Monta cfg igual ao handler principal
    const cfg = buildConfigFromEvent(originalEvent, context);
    if (cfg.error) {
      throw Object.assign(new Error(cfg.error.msg), { statusCode: cfg.error.status });
    }

    // 2) Prepara arquivo (mesma lógica do handler.js)
    const gerarPreview = false; // aqui não precisa preview
    const limitePreview = 0;
    const limitarLinhas = cfg.general.limit > 0 ? cfg.general.limit : undefined;

    const preparo = await prepararArquivo({
      event: originalEvent,
      headers: cfg.headers,
      gerarPreview,
      limitePreview,
      limitarLinhas,
      formatarPreview: null,
    });

    const linhasArquivo = Array.isArray(preparo?.linhas)
      ? preparo.linhas
      : (preparo?.linhas ? [preparo.linhas] : []);

    totalLinhas = linhasArquivo.length;

    // 3) Template + constantes/sanitização
    const t = applyTemplateIfAny({
      linhas: linhasArquivo,
      event: originalEvent,
      headers: cfg.headers,
    });

    const { consts, registrosProcessados } = applyConstantesAndSanitize({
      registros: t.linhas,
      headersLower: cfg.headers,
      headersRaw: cfg.headersRaw,
      overrideConsts: String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true',
      usouTemplate: t.usouTemplate,
      modoDireto: cfg.direct.useDirect,
    });

    // 4) Modo Direto x Integração, reaproveitando planos
    let resumo;
    if (cfg.direct.useDirect) {
      const plano = buildPlanoDireto({
        registrosProcessados,
        cfg,
        totalLinhas: t.linhas.length,
        previewAtivo: false,
      });

      // Aqui você pode aumentar o orçamento, sem mexer no arquivo do serviço:
      resumo = await executarEnvioDireto({
        ...plano.args,
        apigwSoftTimeoutMs: 14 * 60 * 1000,  // 14 min de “janela”
        timeoutMs: 13 * 60 * 1000,           // 13 min por request
      });
    } else {
      const plano = buildPlanoIntegracao({
        linhasParaEnviar: t.linhas,
        resumoArquivo: {
          filename: preparo.arquivo.filename,
          contentType: preparo.arquivo.contentType,
          uploadId: cfg.ids.uploadId,
          fileHash: cfg.ids.fileHash,
        },
        cfg,
      });

      resumo = await executarEnvioIntegracao(plano.args);
    }

    enviadosOk   = resumo.recordsInserted + resumo.recordsUpdated;
    enviadosErro = resumo.skippedDuplicates || 0; // ou alguma métrica de erro que você preferir

  } catch (e) {
    console.error('Erro no asyncWorker:', e);
    statusFinal = 'FAILED';
    erroGeral = e.message || String(e);
  }

  // 5) Callback para seu sistema (se tiver URL)
  if (callbackUrl) {
    const payloadCallback = {
      correlationId,
      finalizado: true,
      status: statusFinal,
      totalRegistros: totalLinhas,
      registrosOk: enviadosOk,
      registrosErro: enviadosErro,
      erroGeral,
    };

    try {
      await httpJson(callbackUrl, 'POST', {}, payloadCallback, 15000);
    } catch (e) {
      console.error('Falha ao chamar callback:', e.message || e);
      // sem Dynamo/SQS, aqui a responsabilidade de reprocessar é sua, via logs
    }
  }

  return; // resposta da worker não vai pra API Gateway
};

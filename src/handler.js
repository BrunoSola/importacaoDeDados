// src/handler.js
// Lambda Importadora genérica
const { respostaJson } = require('./utils/httpResponse');
const { prepararArquivo } = require('./services/arquivoService');
const { extrairConstantes } = require('./utils/constantes');

const { buildConfigFromEvent } = require('./handler.parts/headers');
const { applyTemplateIfAny, applyConstantesAndSanitize } = require('./handler.parts/template');
const { buildPlanoDireto, buildPlanoIntegracao } = require('./handler.parts/plan');

const { executarEnvioDireto } = require('./services/envioDiretoService');
const { executarEnvioIntegracao } = require('./services/envioIntegracaoService');

async function handler(event, context) {
  if (context && typeof context.callbackWaitsForEmptyEventLoop === 'boolean') {
    context.callbackWaitsForEmptyEventLoop = false;
  }

  const cfg = buildConfigFromEvent(event, context);
  if (cfg.error) {
    return respostaJson(cfg.error.status, { error: cfg.error.msg });
  }

  try {
    // 1) Preparo de arquivo (respeita preview/limit)
    const gerarPreview = cfg.flags.preview;
    const limitePreview = 5;
    const limitarLinhas = cfg.general.limit > 0 ? cfg.general.limit : undefined;

    const preparo = await prepararArquivo({
      event,
      headers: cfg.headers,
      gerarPreview,
      limitePreview,
      limitarLinhas,
      formatarPreview: (registros) => {
        const arr = Array.isArray(registros) ? registros : (registros ? [registros] : []);
        // aplica somente constantes, sem sanitizar aqui (sanitização é pós-template)
        const { aplicarConstantes } = require('./utils/constantes');
        const overrideConsts = String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true';
        return arr.map((registro) => aplicarConstantes(registro, extrairConstantes(cfg.headers, cfg.headersRaw), overrideConsts));
      },
    });

    const linhasArquivo = Array.isArray(preparo?.linhas) ? preparo.linhas : (preparo?.linhas ? [preparo.linhas] : []);
    const { filename, contentType } = preparo.arquivo;

    // 2) Template (se houver) + 3) Constantes + sanitização (apenas se usou template)
    const t = applyTemplateIfAny({ linhas: linhasArquivo, event, headers: cfg.headers });
    const { consts, registrosProcessados } = applyConstantesAndSanitize({
      registros: t.linhas,
      headersLower: cfg.headers,
      headersRaw: cfg.headersRaw,
      overrideConsts: String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true',
      usouTemplate: t.usouTemplate,
    });

    // 4) Decisão de modo → montar plano e executar
    if (cfg.direct.useDirect) {
      const plano = buildPlanoDireto({
        registrosProcessados,
        cfg,
        totalLinhas: t.linhas.length,
        previewAtivo: cfg.flags.preview,
      });
      return executarEnvioDireto(plano.args);
    }

    const plano = buildPlanoIntegracao({
      linhasParaEnviar: t.linhas,
      resumoArquivo: { filename, contentType, uploadId: cfg.ids.uploadId, fileHash: cfg.ids.fileHash },
      cfg,
    });
    return executarEnvioIntegracao(plano.args);

  } catch (err) {
    return respostaJson(err.statusCode || 500, { error: err.message || 'Erro inesperado' });
  }
}

module.exports = { handler };

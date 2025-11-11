// src/handler.parts/plan.js
function buildPlanoDireto({
  registrosProcessados,
  cfg, totalLinhas, previewAtivo,
}) {
  const { env, ids, offsets, direct, auth, contexto } = cfg;
  return {
    tipo: 'direto',
    args: {
      registros: registrosProcessados,
      offsetInicial: offsets.startOffset,
      endpointUrl: direct.directEndpointUrl,
      token: auth.authorization,
      batchSize: direct.directBatchSize,
      timeoutMs: env.DEFAULT_TIMEOUT_MS,
      iniciouEm: contexto.iniciouEm,
      previewAtivo,
      uploadId: ids.uploadId,
      fileHash: ids.fileHash,
      margemSegurancaMs: env.SAFE_MS,
      totalLinhas,
      dryRun: cfg.flags.dryRun,
      contextoLambda: contexto.context,
      apigwSoftTimeoutMs: env.APIGW_SOFT_TIMEOUT_MS,
    }
  };
}

function buildPlanoIntegracao({
  linhasParaEnviar, resumoArquivo, cfg,
}) {
  const { env, headersRaw, headers, flags, ids, offsets, integracao, auth, contexto } = cfg;

  return {
    tipo: 'integracao',
    args: {
      linhas: linhasParaEnviar,
      startOffset: offsets.startOffset,
      batchSize: integracao.batchSizeIntegracao,
      concurrency: env.CONCURRENCY,
      // consts/override e sanitização serão aplicados no serviço
      consts: null, // deixa o serviço decidir se quer re-aplicar
      overrideConsts: String(headers['x-override-consts'] || '').toLowerCase() === 'true',
      dryRun: flags.dryRun,
      endpoint: integracao.endpoint,
      authorization: auth.authorization,
      integrationUrl: env.INTEGRATION_URL,
      timeoutMs: env.DEFAULT_TIMEOUT_MS,
      logProgress: flags.logProgress,
      stopOnError: flags.stopOnError,
      contextoLambda: contexto.context,
      margemSegurancaMs: env.SAFE_MS,
      iniciouEm: contexto.iniciouEm,
      previewAtivo: flags.preview,
      resumoArquivo,
      // injetamos utilitários no handler original
      httpJson: require('../core/httpClient').httpJson,
      aplicarConstantes: require('../utils/constantes').aplicarConstantes,
      sanitizarRegistro: require('../utils/registros').limparRegistroPlano,
    }
  };
}

module.exports = { buildPlanoDireto, buildPlanoIntegracao };

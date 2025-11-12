// src/handler.parts/plan.js
//
// Montagem dos “planos” de execução a partir dos headers/env.
// Regras em português:
// - x-batch-size: tamanho nominal do lote. Se 0/≤0, usa o padrão calculado (cfg.direct.batchSize ou 20).
// - x-suggest-batch-size: controle adaptativo. 0 desativa; >0 substitui o nominal TEMPORARIAMENTE.
// - Este arquivo só prepara os “args” para os serviços; a lógica de envio está em envioDiretoService/envioIntegracaoService.
//
// Observação: manter as mudanças isoladas do modo integração para não afetar outros importadores.

function buildPlanoDireto({ registrosProcessados, cfg, totalLinhas, previewAtivo }) {
  // Leitura dos headers relevantes
  const hdrBatch = Number(cfg.headers['x-batch-size']);
  const hdrSuggest = Number(cfg.headers['x-suggest-batch-size']);

  // Padrão vindo da própria config (já derivado de env/handler)
  const defaultBatchFromCfg = Math.max(1, Number(cfg.direct.batchSize || 20));

  // Se x-batch-size ≤ 0, usamos o padrão do código
  const batchSizeNominal =
    Number.isFinite(hdrBatch) && hdrBatch > 0 ? hdrBatch : defaultBatchFromCfg;

  // Sugerido: só vale se > 0 (0 desativa)
  const batchSizeSugerido =
    Number.isFinite(hdrSuggest) && hdrSuggest > 0 ? hdrSuggest : 0;

  const args = {
    registros: registrosProcessados,
    offsetInicial: Math.max(0, parseInt(cfg.headers['x-offset'] || '0', 10)),
    endpointUrl: cfg.direct.endpointUrl,
    token: cfg.integration.authorization,

    // Nominal (estável) e sugerido (adaptativo)
    batchSize: batchSizeNominal,
    batchSizeSugerido,

    timeoutMs: cfg.general.defaultTimeoutMs,
    iniciouEm: cfg.general.iniciouEm,
    previewAtivo,
    uploadId: cfg.ids.uploadId,
    fileHash: cfg.ids.fileHash,
    margemSegurancaMs: cfg.general.safeRemainingMs,
    totalLinhas,
    dryRun: cfg.flags.dryRun,
    contextoLambda: cfg.context,
    apigwSoftTimeoutMs: cfg.general.apigwSoftTimeoutMs,
  };
  return { args };
}

function buildPlanoIntegracao({ linhasParaEnviar, resumoArquivo, cfg }) {
  // Integração permanece inalterada para não impactar outros modelos de importação.
  const args = {
    linhas: linhasParaEnviar,
    startOffset: Math.max(0, parseInt(cfg.headers['x-offset'] || '0', 10)),
    batchSize: cfg.general.batchSize,
    concurrency: cfg.general.concurrency,
    consts: null,
    overrideConsts: String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true',
    dryRun: cfg.flags.dryRun,
    endpoint: cfg.integration.endpoint,
    authorization: cfg.integration.authorization,
    integrationUrl: cfg.integration.integrationUrl,
    timeoutMs: cfg.general.defaultTimeoutMs,
    logProgress: cfg.flags.logProgress,
    stopOnError: cfg.flags.stopOnError,
    contextoLambda: cfg.context,
    margemSegurancaMs: cfg.general.safeRemainingMs,
    iniciouEm: cfg.general.iniciouEm,
    previewAtivo: cfg.flags.preview,
    resumoArquivo,
  };
  return { args };
}

module.exports = { buildPlanoDireto, buildPlanoIntegracao };

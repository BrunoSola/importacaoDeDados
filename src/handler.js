// src/handler.js
// Lambda Importadora genérica
//
// Objetivo deste handler:
// 1) Ler/parsear o arquivo recebido (JSON base64 ou multipart), com suporte a preview/limit;
// 2) (Opcional) Aplicar template para construir o payload final (single ou um por linha);
// 3) Aplicar constantes (e sanitizar quando usou template);
// 4) Decidir o modo de envio (Direto x Integração) e executar;
// 5) Retornar resposta padronizada (200/206/erro), dentro da janela do API Gateway.
//
// Observação: toda a leitura de headers/env e montagem de config está em handler.parts/headers.

const { respostaJson } = require('./utils/httpResponse');
const { prepararArquivo } = require('./services/arquivoService');
const { extrairConstantes } = require('./utils/constantes');

const { buildConfigFromEvent } = require('./handler.parts/headers');
const { applyTemplateIfAny, applyConstantesAndSanitize } = require('./handler.parts/template');
const { buildPlanoDireto, buildPlanoIntegracao } = require('./handler.parts/plan');

const { executarEnvioDireto } = require('./services/envioDiretoService');
const { executarEnvioIntegracao } = require('./services/envioIntegracaoService');

async function handler(event, context) {
  // Boa prática para Lambda Node: não esperar o event loop esvaziar ao finalizar.
  if (context && typeof context.callbackWaitsForEmptyEventLoop === 'boolean') {
    context.callbackWaitsForEmptyEventLoop = false;
  }

  // Centraliza leitura/normalização de headers/env/flags.
  // Se algo essencial estiver faltando (ex.: Authorization), já retorna erro padronizado.
  const cfg = buildConfigFromEvent(event, context);
  if (cfg.error) {
    return respostaJson(cfg.error.status, { error: cfg.error.msg });
  }

  try {
    // (1) PREPARO DE ARQUIVO
    // - Lê arquivo do body (buffer/nome/tipo), detecta CSV/XLSX/XML (NFe tem caminho especial).
    // - Respeita preview (pequena amostra) e x-limit/limitarLinhas (corte total para testes).
    // - formatarPreview permite aplicar transformações LEVES na amostra (ex.: constantes),
    //   sem sanitizar aqui (sanitização ocorre mais adiante, após aplicar template).
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
        // Aplica APENAS constantes nesta prévia.
        // Observação: sanitização será feita depois, se houver template.
        const { aplicarConstantes } = require('./utils/constantes');
        const overrideConsts = String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true';
        return arr.map((registro) =>
          aplicarConstantes(registro, extrairConstantes(cfg.headers, cfg.headersRaw), overrideConsts)
        );
      },
    });

    // Garante que teremos um array de linhas; captura metadados do arquivo para logs/resumo.
    const linhasArquivo = Array.isArray(preparo?.linhas) ? preparo.linhas : (preparo?.linhas ? [preparo.linhas] : []);
    const { filename, contentType } = preparo.arquivo;

    // (2) TEMPLATE (SE HOUVER) → constrói payload(s)
    // (3) CONSTANTES + SANITIZAÇÃO
    // - applyTemplateIfAny: busca template em header/body e constrói os payloads
    //   (single ou um por linha), logando fonte e opções.
    // - applyConstantesAndSanitize: aplica constantes SEMPRE e sanitiza APENAS quando usou template.
    const t = applyTemplateIfAny({ linhas: linhasArquivo, event, headers: cfg.headers });
    const { consts, registrosProcessados } = applyConstantesAndSanitize({
      registros: t.linhas,
      headersLower: cfg.headers,
      headersRaw: cfg.headersRaw,
      overrideConsts:
        String(cfg.headers['x-override-consts'] || '').toLowerCase() === 'true',
      usouTemplate: t.usouTemplate,
      modoDireto: cfg.direct.useDirect,
    });

    // (4) DECISÃO DE MODO → monta plano e executa
    // - Direto: envia em lotes para endpoint Flowch (x-endpoint-url).
    // - Integração: envia um a um (ou em pequenos grupos) para sua lambda integradora (INTEGRATION_URL + endpoint).
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
    // (5) TRATAMENTO DE ERRO PADRONIZADO
    // - Mantém statusCode se já estiver setado; caso contrário, 500 genérico.
    return respostaJson(err.statusCode || 500, { error: err.message || 'Erro inesperado' });
  }
}

module.exports = { handler };

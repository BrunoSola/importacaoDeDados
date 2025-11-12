const { respostaJson } = require('../utils/httpResponse');
const { mapearComLimite } = require('../utils/concurrency');
const { mediaNumeros, percentil } = require('../utils/normalizadores');

// ✅ Fallbacks locais (caso o handler não injete dependências)
const { httpJson: httpJsonDefault } = require('../core/httpClient');
const { aplicarConstantes: aplicarConstantesDefault } = require('../utils/constantes');

/**
 * Aguarda "ms" milissegundos (throttle opcional entre lotes)
 */
function aguardar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Envio via LAMBDA DE INTEGRAÇÃO (modo "integração").
 * - Recebe os registros, aplica constantes + sanitização e envia 1 por vez para a lambda de integração.
 * - Concurrency controlado por "mapearComLimite".
 * - Retorna 206 quando:
 *    • o tempo restante da Lambda está curto (protege a janela do API Gateway),
 *    • útil para retomar do "nextOffset" sem perder o progresso já enviado.
 *
 * Observações importantes:
 * - Este serviço aceita funções injetadas (httpJson, aplicarConstantes, sanitizarRegistro).
 *   Se não vierem, usamos FALLBACKS internos, evitando "is not a function".
 * - "integrationUrl" é a URL da sua lambda de integração; o "endpoint" vai no header,
 *   como você já usa no seu integrador.
 */
async function executarEnvioIntegracao({
  linhas,
  startOffset,
  batchSize,
  concurrency,
  consts,
  overrideConsts,
  dryRun,
  endpoint,
  authorization,
  integrationUrl,
  timeoutMs,
  logProgress,
  stopOnError,
  contextoLambda,
  margemSegurancaMs,
  iniciouEm,
  previewAtivo,
  resumoArquivo,

  // ✅ Dependências opcionais injetadas pelo handler (com fallback seguro)
  httpJson,
  aplicarConstantes,
  sanitizarRegistro,
}) {
  // ---------- NORMALIZAÇÕES & FALLBACKS ----------
  // Dependências: se não vierem, usamos padrões
  const _httpJson = httpJson || httpJsonDefault;
  const _aplicarConstantes = aplicarConstantes || aplicarConstantesDefault;
  const _sanitizarRegistro = sanitizarRegistro || ((x) => x);

  // Batch/concurrency: garantem valores > 0 (evita loop infinito/estouro)
  const loteBase = Number.isFinite(Number(batchSize)) ? Number(batchSize) : Number(process.env.BATCH_SIZE || 5);
  const lote = Math.max(1, loteBase);
  const concBase = Number.isFinite(Number(concurrency)) ? Number(concurrency) : Number(process.env.CONCURRENCY || 5);
  const conc = Math.max(1, concBase);

  // Autorização/URL/endpoint: validações mínimas
  const authHeader = String(authorization || '').trim();
  if (!integrationUrl) {
    return respostaJson(500, { error: 'INTEGRAÇÃO: "integrationUrl" não configurada.' });
  }
  if (!endpoint) {
    return respostaJson(400, { error: 'INTEGRAÇÃO: Header "endpoint" é obrigatório.' });
  }
  if (!authHeader) {
    return respostaJson(400, { error: 'INTEGRAÇÃO: Header "Authorization" é obrigatório.' });
  }

  // ---------- MÉTRICAS DA EXECUÇÃO ----------
  let enviados = 0;      // quantos 2xx
  let jaExistiam = 0;    // "ALREADY_EXISTS" sinalizado pela integração
  let erros = 0;         // respostas não-2xx ou exceções
  const amostrasErro = [];// primeiros erros para log/diagnóstico

  // Cursor de leitura do array de linhas
  const total = Array.isArray(linhas) ? linhas.length : 0;
  const inicioCursor = Math.min(Math.max(0, Number(startOffset || 0)), total);

  // ---------- LOOP POR LOTES ----------
  for (let offset = inicioCursor; offset < total; offset += lote) {
    const fatia = linhas.slice(offset, offset + lote);

    // DRY-RUN: não envia nada, mas mantém contadores e perfis
    if (dryRun) {
      enviados += fatia.length;
      if (logProgress) {
        console.log(JSON.stringify({
          progress: { processed: Math.min(offset + fatia.length, total), total, sent: enviados, already: jaExistiam, errors: erros },
          batchPerf: { count: 0, avgMs: 0, p95Ms: 0 },
        }));
      }
    } else {
      // Monta payloads (aplica constantes + sanitização)
      const payloads = fatia.map((registro) => {
        const comConst = _aplicarConstantes(registro, consts, !!overrideConsts);
        return _sanitizarRegistro(comConst);
      });

      // Envia em paralelo limitado
      const resultadosLote = await mapearComLimite(payloads, conc, async (payload, indiceNoLote) => {
        const indiceGlobal = offset + indiceNoLote;
        const linhaExcel = indiceGlobal + 2; // header na linha 1

        try {
          const inicio = Date.now();
          const resposta = await _httpJson(
            integrationUrl, // sua lambda integração recebe "endpoint" no header
            'POST',
            {
              'Content-Type': 'application/json',
              'Authorization': authHeader,
              'endpoint': endpoint,
            },
            JSON.stringify(payload),
            timeoutMs,
          );
          const duracao = Date.now() - inicio;

          // Tenta interpretar o body
          let corpo = {};
          try { corpo = JSON.parse(resposta.body || '{}'); } catch { corpo = { raw: resposta.body }; }

          // Sinal especial da sua integração para duplicata
          if (resposta.statusCode === 200 && corpo?.message === 'ALREADY_EXISTS') {
            jaExistiam++;
            return {
              linha: linhaExcel,
              endpoint,
              status: 'ALREADY_EXISTS',
              mensagem: 'Registro já existia no destino.',
              upstreamStatus: resposta.statusCode,
              perfMs: duracao,
            };
          }

          // Sucesso genérico (2xx)
          if (resposta.statusCode >= 200 && resposta.statusCode < 300) {
            enviados++;
            return {
              linha: linhaExcel,
              endpoint,
              status: 'ENVIADO',
              mensagem: 'OK',
              upstreamStatus: resposta.statusCode,
              perfMs: duracao,
            };
          }

          // Erro HTTP (não-2xx)
          erros++;
          const mensagem = corpo?.error || (typeof corpo === 'string' ? corpo : JSON.stringify(corpo));
          const erroObj = {
            linha: linhaExcel,
            endpoint,
            status: 'ERRO',
            mensagem,
            upstreamStatus: resposta.statusCode,
            upstreamBody: corpo,
            perfMs: duracao,
          };
          if (amostrasErro.length < 5) amostrasErro.push(erroObj);
          if (stopOnError) throw new Error(`[STOP_ON_ERROR] ${mensagem}`);
          return erroObj;

        } catch (erro) {
          // Exceção de rede/timeout/etc.
          erros++;
          const erroObj = {
            linha: linhaExcel,
            endpoint,
            status: 'ERRO',
            mensagem: erro.message || 'Falha ao enviar',
          };
          if (amostrasErro.length < 5) amostrasErro.push(erroObj);
          if (stopOnError) throw erro;
          return erroObj;
        }
      });

      // Log de progresso e métricas de perf do lote
      if (logProgress) {
        const tempos = resultadosLote
          .map(r => (typeof r?.perfMs === 'number' ? r.perfMs : null))
          .filter(v => v !== null && v >= 0);

        console.log(JSON.stringify({
          progress: {
            processed: Math.min(offset + fatia.length, total),
            total,
            sent: enviados,
            already: jaExistiam,
            errors: erros,
          },
          batchPerf: {
            count: tempos.length,
            avgMs: mediaNumeros(tempos),
            p95Ms: percentil(tempos, 95),
          },
        }));
      }
    }

    // Throttle entre lotes (opcional via env)
    const sleepMs = Number(process.env.BATCH_SLEEP_MS || 0);
    if (sleepMs > 0 && offset + fatia.length < total) {
      await aguardar(sleepMs);
    }

    // Proteção de tempo restante (retorna 206 com estado parcial)
    if (
      contextoLambda &&
      typeof contextoLambda.getRemainingTimeInMillis === 'function' &&
      contextoLambda.getRemainingTimeInMillis() <= margemSegurancaMs
    ) {
      const duracao = Date.now() - iniciouEm;
      const proximoOffset = Math.min(offset + fatia.length, total);

      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: proximoOffset, // retoma exatamente do próximo bloco
          done: false,
          summary: {
            arquivo: resumoArquivo?.filename,
            contentType: resumoArquivo?.contentType,
            linhasLidas: total,
            enviadas: enviados,
            jaExistiam,
            erros,
            duracaoMs: duracao,
            dryRun: !!dryRun,
            preview: !!previewAtivo,
            paralelismo: conc,
            batchSize: lote,
            uploadId: resumoArquivo?.uploadId,
            fileHash: resumoArquivo?.fileHash,
            amostrasErro, // ajuda a debugar rapidamente
          },
        }),
      };
    }

    // Se for "pare no primeiro erro", já interrompe o loop de lotes
    if (stopOnError && erros > 0 && !dryRun) break;
  }

  // ---------- RESUMO FINAL ----------
  const duracao = Date.now() - iniciouEm;

  return respostaJson(200, {
    nextOffset: null,
    done: true,
    summary: {
      arquivo: resumoArquivo?.filename,
      contentType: resumoArquivo?.contentType,
      linhasLidas: Array.isArray(linhas) ? linhas.length : 0,
      enviadas: enviados,
      jaExistiam,
      erros,
      duracaoMs: duracao,
      dryRun: !!dryRun,
      preview: !!previewAtivo,
      paralelismo: conc,
      batchSize: lote,
      uploadId: resumoArquivo?.uploadId,
      fileHash: resumoArquivo?.fileHash,
      amostrasErro,
    },
  });
}

module.exports = {
  executarEnvioIntegracao,
};

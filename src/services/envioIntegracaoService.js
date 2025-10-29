const { respostaJson } = require('../utils/httpResponse');
const { mapearComLimite } = require('../utils/concurrency');
const { mediaNumeros, percentil } = require('../utils/normalizadores');


function aguardar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
  httpJson,
  aplicarConstantes,
  sanitizarRegistro,
}) {
  let enviados = 0;
  let jaExistiam = 0;
  let erros = 0;
  const amostrasErro = [];

  for (let offset = Math.min(startOffset, linhas.length); offset < linhas.length; offset += batchSize) {
    const lote = linhas.slice(offset, offset + batchSize);

    const resultadosLote = await mapearComLimite(lote, concurrency, async (registro, indiceNoLote) => {
      const indiceGlobal = offset + indiceNoLote;
      const linhaExcel = indiceGlobal + 2;

      let payload = aplicarConstantes(registro, consts, overrideConsts);
      payload = sanitizarRegistro(payload);

      if (dryRun) {
        enviados++;
        return {
          linha: linhaExcel,
          endpoint,
          status: 'ENVIADO (DRY-RUN)',
          mensagem: 'Simulação – não enviado.',
          perfMs: 0,
        };
      }

      try {
        const inicio = Date.now();
        const resposta = await httpJson(
          integrationUrl,
          'POST',
          {
            'Content-Type': 'application/json',
            'Authorization': authorization,
            'endpoint': endpoint,
          },
          JSON.stringify(payload),
          timeoutMs,
        );
        const duracao = Date.now() - inicio;

        let corpo = {};
        try { corpo = JSON.parse(resposta.body || '{}'); } catch { corpo = { raw: resposta.body }; }

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

    if (logProgress) {
      const tempos = resultadosLote
        .map(r => (typeof r?.perfMs === 'number' ? r.perfMs : null))
        .filter(v => v !== null && v >= 0);

      console.log(JSON.stringify({
        progress: {
          processed: Math.min(offset + lote.length, linhas.length),
          total: linhas.length,
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

    const sleepMs = Number(process.env.BATCH_SLEEP_MS || 0);
    if (sleepMs > 0 && offset + lote.length < linhas.length) {
      await aguardar(sleepMs);
    }

    if (
      contextoLambda &&
      typeof contextoLambda.getRemainingTimeInMillis === 'function' &&
      contextoLambda.getRemainingTimeInMillis() <= margemSegurancaMs
    ) {
      const duracao = Date.now() - iniciouEm;
      const proximoOffset = Math.min(offset + lote.length, linhas.length);
      return {
        statusCode: 206,
        headers: { 'Content-Type': 'application/json', 'Retry-After': '1' },
        body: JSON.stringify({
          nextOffset: proximoOffset,
          done: false,
          summary: {
            arquivo: resumoArquivo.filename,
            contentType: resumoArquivo.contentType,
            linhasLidas: linhas.length,
            enviadas: enviados,
            jaExistiam,
            erros,
            duracaoMs: duracao,
            dryRun: !!dryRun,
            preview: !!previewAtivo,
            paralelismo: concurrency,
            batchSize,
            uploadId: resumoArquivo.uploadId,
            fileHash: resumoArquivo.fileHash,
          },
        }),
      };
    }

    if (stopOnError && erros > 0 && !dryRun) break;
  }

  const duracao = Date.now() - iniciouEm;

  return respostaJson(200, {
    nextOffset: null,
    done: true,
    summary: {
      arquivo: resumoArquivo.filename,
      contentType: resumoArquivo.contentType,
      linhasLidas: linhas.length,
      enviadas: enviados,
      jaExistiam,
      erros,
      duracaoMs: duracao,
      dryRun: !!dryRun,
      preview: !!previewAtivo,
      paralelismo: concurrency,
      batchSize,
      uploadId: resumoArquivo.uploadId,
      fileHash: resumoArquivo.fileHash,
    },
  });
}

module.exports = {
  executarEnvioIntegracao,
};

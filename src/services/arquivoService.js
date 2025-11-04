// src/services/arquivoService.js

const { readBodyBase64, toLowerHeaders } = require('../utils/parseEvent');
const { parseFileToObjects, detectKind } = require('../utils/fileParser');
const { looksLikeNFe, transformNfe  } = require('../transformers/nfe');

function pareceTexto(buffer) {
  const head = buffer.slice(0, Math.min(buffer.length, 4096));
  const possuiNulo = head.includes(0x00);
  const possuiQuebra = head.includes(0x0A) || head.includes(0x0D);
  return !possuiNulo && possuiQuebra;
}

function detectarContentType(buffer) {
  if (buffer.length >= 2 && buffer[0] === 0x50 && buffer[1] === 0x4B) {
    return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  }
  const head = buffer.slice(0, 64).toString('utf8').trim();
  if (head.startsWith('<')) return 'application/xml';
  if (pareceTexto(buffer)) return 'text/csv';
  return 'application/octet-stream';
}

function nomePadrao(contentType) {
  if ((contentType || '').includes('spreadsheetml')) return 'upload.xlsx';
  if ((contentType || '').startsWith('text/')) return 'upload.csv';
  return 'upload.bin';
}

async function prepararArquivo({
  event,
  headers,
  gerarPreview = false,
  limitePreview = 5,
  limitarLinhas,
  formatarPreview,
}) {
  const arquivo = readBodyBase64(event);
  let { contentType, filename, buffer } = arquivo;

  if (!buffer?.length) {
    const e = new Error('Arquivo ausente no body (base64/multipart) ou vazio.');
    e.statusCode = 400;
    throw e;
  }

  const tipoInferido = detectarContentType(buffer);
  if (!contentType || contentType === 'application/json') {
    contentType = tipoInferido;
  }
  if (!filename) {
    filename = nomePadrao(contentType);
  }

  console.log('debug-upload', {
    contentType,
    filename,
    size: buffer?.length,
    headHex: buffer?.slice(0, 8)?.toString('hex'),
  });

  // NF-e: transforma direto do XML
  if (looksLikeNFe({ contentType, filename, buffer })) {
    const linhas = await transformNfe({ xmlBuffer: buffer, filename });
    const preview = gerarPreview && typeof formatarPreview === 'function'
      ? formatarPreview(linhas.slice(0, limitePreview))
      : undefined;

    return {
      tipo: 'xml-nfe',
      linhas,
      preview,
      arquivo: { buffer, contentType, filename, tamanho: buffer.length },
    };
  }

  // Demais tipos: CSV/XLSX via parser genérico
  const h = toLowerHeaders(headers || {});
  const limitarLinhasHeader = Number(h['x-limit'] || 0) || 0;
  const limitarLinhasFinal = Number(limitarLinhas) > 0 ? Number(limitarLinhas) : limitarLinhasHeader;
  let linhas;
  try {
    linhas = await parseFileToObjects({ buffer, contentType, filename, headers: h, limitarLinhas: limitarLinhasFinal });
    console.log('file-kind', { decidedKind: detectKind({ contentType, filename }) });
  } catch (erro) {
    throw erro;
  }

  if (!linhas.length) {
    const e = new Error('Sem linhas de dados (verifique o cabeçalho na primeira linha).');
    e.statusCode = 400;
    throw e;
  }

  if (limitarLinhasFinal  > 0) {
    linhas = linhas.slice(0, limitarLinhasFinal);
  }

  const preview = gerarPreview && typeof formatarPreview === 'function'
    ? formatarPreview(linhas.slice(0, limitarLinhasFinal))
    : undefined;

  return {
    tipo: detectKind({ contentType, filename }),
    linhas,
    preview,
    arquivo: {
      buffer,
      contentType,
      filename,
      tamanho: buffer.length,
    },
  };
}


module.exports = {
  prepararArquivo,
};

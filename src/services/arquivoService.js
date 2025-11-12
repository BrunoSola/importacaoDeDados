// src/services/arquivoService.js
//
// Serviço responsável por:
// - Ler o arquivo enviado no body (JSON base64 ou multipart) e extrair buffer/nome/tipo
// - Detectar o tipo real (XLSX, CSV, XML) de forma robusta (magic number / heurísticas)
// - Converter o arquivo em um array de objetos (linhas), respeitando x-limit quando houver
// - Tratar caso especial de NF-e (XML), transformando diretamente em registros
// - Gerar um preview opcional (até limitePreview linhas), aplicando o formatarPreview recebido

const { readBodyBase64, toLowerHeaders } = require('../utils/parseEvent');
const { parseFileToObjects, detectKind } = require('../utils/fileParser');
const { looksLikeNFe, transformNfe  } = require('../transformers/nfe');

/**
 * Heurística simples para “parece texto”:
 * - Não contém byte nulo nos primeiros KB
 * - Contém quebras de linha
 * Útil para diferenciar CSV de binário quando o content-type não é confiável.
 */
function pareceTexto(buffer) {
  const head = buffer.slice(0, Math.min(buffer.length, 4096));
  const possuiNulo = head.includes(0x00);
  const possuiQuebra = head.includes(0x0A) || head.includes(0x0D);
  return !possuiNulo && possuiQuebra;
}

/**
 * Detecção de content-type a partir do conteúdo (fallback confiável):
 * - XLSX: começa com “PK\x03\x04”
 * - XML: inicia com “<”
 * - CSV: texto simples com quebras
 * - Senão: binário genérico
 */
function detectarContentType(buffer) {
  if (buffer.length >= 2 && buffer[0] === 0x50 && buffer[1] === 0x4B) {
    return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  }
  const head = buffer.slice(0, 64).toString('utf8').trim();
  if (head.startsWith('<')) return 'application/xml';
  if (pareceTexto(buffer)) return 'text/csv';
  return 'application/octet-stream';
}

/**
 * Gera um nome padrão quando o uploader não enviou filename:
 * - Planilhas → upload.xlsx
 * - Texto → upload.csv
 * - Genérico → upload.bin
 */
function nomePadrao(contentType) {
  if ((contentType || '').includes('spreadsheetml')) return 'upload.xlsx';
  if ((contentType || '').startsWith('text/')) return 'upload.csv';
  return 'upload.bin';
}

/**
 * Função principal deste serviço.
 * Parâmetros:
 * - event: evento bruto (com body/base64)
 * - headers: cabeçalhos originais (usados para x-limit, etc.)
 * - gerarPreview: se true, calcula prévia de N linhas
 * - limitePreview: quantidade de linhas da prévia (default 5)
 * - limitarLinhas: permite limitar o total processado (prioridade maior que x-limit)
 * - formatarPreview: callback para transformar as linhas do preview (ex.: aplicar constantes)
 *
 * Retorno:
 * {
 *   tipo: 'csv' | 'xlsx' | 'xml' | 'xml-nfe',
 *   linhas: Array<Object>,
 *   preview?: Array<Object>,
 *   arquivo: { buffer, contentType, filename, tamanho }
 * }
 */
async function prepararArquivo({
  event,
  headers,
  gerarPreview = false,
  limitePreview = 5,
  limitarLinhas,
  formatarPreview,
}) {
  // Lê o body e tenta extrair { contentType, filename, buffer } (JSON base64 ou multipart)
  const arquivo = readBodyBase64(event);
  let { contentType, filename, buffer } = arquivo;

  // Validação mínima: precisa existir buffer com conteúdo
  if (!buffer?.length) {
    const e = new Error('Arquivo ausente no body (base64/multipart) ou vazio.');
    e.statusCode = 400;
    throw e;
  }

  // Se o content-type veio genérico/ausente (ex.: application/json), detecta a partir do conteúdo
  const tipoInferido = detectarContentType(buffer);
  if (!contentType || contentType === 'application/json') {
    contentType = tipoInferido;
  }

  // Se não veio filename, define um padrão coerente com o tipo detectado
  if (!filename) {
    filename = nomePadrao(contentType);
  }

  // Detecta o “kind” (csv/xlsx/xml) uma única vez (evita recomputar)
  const decidedKind = detectKind({ contentType, filename });

  // Log de diagnóstico (não sensível); headHex só dos 8 primeiros bytes
  console.log('debug-upload', {
    contentType,
    filename,
    size: buffer.length,
    headHex: buffer.slice(0, 8).toString('hex'),
  });

  // Caso especial: NF-e (XML estruturado) — mapeia direto para objetos sem passar pelo parser genérico
  if (looksLikeNFe({ contentType, filename, buffer })) {
    const linhas = await transformNfe({ xmlBuffer: buffer, filename });

    // Preview: sempre respeita “limitePreview” (não confundir com x-limit)
    const preview = gerarPreview && typeof formatarPreview === 'function'
      ? formatarPreview(linhas.slice(0, Math.min(limitePreview, linhas.length)))
      : undefined;

    return {
      tipo: 'xml-nfe',
      linhas,
      preview,
      arquivo: { buffer, contentType, filename, tamanho: buffer.length },
    };
  }

  // Caminho geral: CSV/XLSX (e XML genérico quando não for NF-e) via parser unificado
  const h = toLowerHeaders(headers || {});
  // “x-limit” do header (usado para limitar processamento total; útil para testes)
  const limitarLinhasHeader = Number(h['x-limit'] || 0) || 0;
  // “limitarLinhas” passado na chamada tem prioridade sobre o header
  const limitarLinhasFinal = Number(limitarLinhas) > 0 ? Number(limitarLinhas) : limitarLinhasHeader;

  // Converte arquivo em objetos (cada linha vira um item no array)
  let linhas = await parseFileToObjects({
    buffer,
    contentType,
    filename,
    headers: h,
    limitarLinhas: limitarLinhasFinal,
  });
  console.log('file-kind', { decidedKind });

  // Sem dados úteis? Sinaliza erro específico para facilitar debug do cabeçalho
  if (!linhas.length) {
    const e = new Error('Sem linhas de dados (verifique o cabeçalho na primeira linha).');
    e.statusCode = 400;
    throw e;
  }

  // Se foi solicitado um corte total (x-limit/limitarLinhas), aplica aqui
  if (limitarLinhasFinal > 0 && linhas.length > limitarLinhasFinal) {
    linhas = linhas.slice(0, limitarLinhasFinal);
  }

  // Preview deve usar “limitePreview” (pequena amostra) — independente do limite total de processamento
  const preview = gerarPreview && typeof formatarPreview === 'function'
    ? formatarPreview(linhas.slice(0, Math.min(limitePreview, linhas.length)))
    : undefined;

  // Retorno padronizado para os demais serviços
  return {
    tipo: decidedKind,
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

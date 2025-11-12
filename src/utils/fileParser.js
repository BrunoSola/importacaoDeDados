// src/utils/fileParser.js
// Responsável por delegar a leitura de arquivos aos importadores específicos.
// Otimizado para: segurança, detecção robusta de tipo e leitura por FAIXA (offset/limit) quando suportado.

const LIMITES = require('../importers/limites');
const csvImporter = require('../importers/csvImporter');
const xlsxImporter = require('../importers/xlsxImporter');
const xmlImporter = require('../importers/xmlImporter');

const IMPORTADORES = {
  csv: csvImporter,
  xlsx: xlsxImporter,
  xml: xmlImporter,
};

/**
 * Heurística leve para detectar XLSX por assinatura ZIP "PK"
 * Evita depender apenas de content-type (muitos clients mandam errado).
 */
function isZipSignatureXlsx(buffer = Buffer.alloc(0), filenameLower = '') {
  // XLSX é um ZIP (50 4B = "PK"). Checamos os 2 primeiros bytes.
  const hasZipMagic = buffer.length >= 2 && buffer[0] === 0x50 && buffer[1] === 0x4b;
  const endsWithXlsx = typeof filenameLower === 'string' && filenameLower.endsWith('.xlsx');
  return hasZipMagic && endsWithXlsx;
}

/**
 * Detecta o "kind" do arquivo de forma resiliente.
 * Prioriza filename, depois content-type e, por fim, assinatura para XLSX.
 */
function detectKind({ contentType = '', filename = '', buffer } = {}) {
  const contentTypeLower = (contentType || '').toLowerCase();
  const filenameLower = (filename || '').toLowerCase();

  // 1) Regras óbvias por extensão / mime comum
  if (contentTypeLower.includes('text/csv') || contentTypeLower.includes('application/csv') || filenameLower.endsWith('.csv')) {
    return 'csv';
  }
  if (contentTypeLower.includes('spreadsheetml') || filenameLower.endsWith('.xlsx')) {
    return 'xlsx';
  }
  if (contentTypeLower.includes('ms-excel') || filenameLower.endsWith('.xls')) {
    return 'xls';
  }
  if (contentTypeLower.includes('application/xml') || contentTypeLower.includes('text/xml') || filenameLower.endsWith('.xml')) {
    return 'xml';
  }

  // 2) Fallback: assinatura mínima para XLSX (ZIP + .xlsx)
  if (isZipSignatureXlsx(buffer, filenameLower)) {
    return 'xlsx';
  }

  return 'unknown';
}

/**
 * Validação de tamanho para mitigar uso excessivo de memória.
 */
function garantirTamanhoSeguro(buffer, maxBytes = LIMITES.MAX_BYTES) {
  if (!buffer || !buffer.length) return;
  if (buffer.length > maxBytes) {
    const err = new Error(`Arquivo maior que o permitido (${buffer.length} bytes > ${maxBytes}).`);
    err.statusCode = 413;
    throw err;
  }
}

/**
 * Leitura padrão: carrega TODO o arquivo e devolve array de objetos.
 * Mantida por compatibilidade (preview, templates agregadores, CSV/XML, etc.).
 */
async function parseFileToObjects({ buffer, contentType, filename, headers, limitarLinhas }) {
  if (!buffer || !buffer.length) return [];
  garantirTamanhoSeguro(buffer);

  const tipoDetectado = detectKind({ contentType, filename, buffer });
  const contexto = { buffer, headers, limites: LIMITES, limitarLinhas };

  if (tipoDetectado === 'xls') {
    const err = new Error('Arquivos .xls não são suportados por segurança. Exporte como .xlsx ou .csv.');
    err.statusCode = 400;
    throw err;
  }

  if (IMPORTADORES[tipoDetectado]) {
    return IMPORTADORES[tipoDetectado].carregar(contexto);
  }

  // Tenta XLSX primeiro (com ExcelJS); se falhar, tenta CSV como fallback seguro
  try {
    return await xlsxImporter.carregar(contexto);
  } catch {
    return csvImporter.carregar(contexto);
  }
}

/**
 * NOVO — Leitura por faixa (offset/limit):
 * Para lotes grandes (ex.: 5.700 linhas), monta somente a janela que será enviada agora,
 * reduzindo CPU, memória e GC. Se o importador não suportar faixa, faz fallback: lê tudo e fatia.
 *
 * Parâmetros:
 * - offset: índice inicial (0-based) das LINHAS DE DADOS (após o cabeçalho) a serem lidas.
 * - limit:  quantidade de linhas a ler; 0 ou ausente = até o fim.
 */
async function parseFileToObjectsRange({ buffer, contentType, filename, headers, offset = 0, limit = 0 }) {
  if (!buffer || !buffer.length) return [];
  garantirTamanhoSeguro(buffer);

  const tipoDetectado = detectKind({ contentType, filename, buffer });
  const contextoFaixa = { buffer, headers, limites: LIMITES, offset, limit };

  if (tipoDetectado === 'xls') {
    const err = new Error('Arquivos .xls não são suportados por segurança. Exporte como .xlsx ou .csv.');
    err.statusCode = 400;
    throw err;
  }

  // Suporte nativo a faixa no XLSX (se o importador expuser carregarFaixa)
  if (tipoDetectado === 'xlsx' && typeof xlsxImporter.carregarFaixa === 'function') {
    return xlsxImporter.carregarFaixa(contextoFaixa);
  }

  // Fallback universal (CSV/XML/unknown): lê tudo e fatia
  const todos = await parseFileToObjects({ buffer, contentType, filename, headers });
  const safeOffset = Math.max(0, Number(offset || 0));
  const safeLimit = Math.max(0, Number(limit || 0));
  const endIndex = safeLimit > 0 ? safeOffset + safeLimit : todos.length;
  return todos.slice(safeOffset, endIndex);
}

module.exports = {
  parseFileToObjects,
  parseFileToObjectsRange, // <- export adicional, backward-compatible
  detectKind,
  constants: LIMITES,
};

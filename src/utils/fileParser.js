// src/utils/fileParser.js
// Responsável por delegar a leitura de arquivos aos importadores específicos.

const LIMITES = require('../importers/limites');
const csvImporter = require('../importers/csvImporter');
const xlsxImporter = require('../importers/xlsxImporter');
const xmlImporter = require('../importers/xmlImporter');

const IMPORTADORES = {
  csv: csvImporter,
  xlsx: xlsxImporter,
  xml: xmlImporter,
};

function detectKind({ contentType = '', filename = '' }) {
  const ct = (contentType || '').toLowerCase();
  const name = (filename || '').toLowerCase();

  if (ct.includes('text/csv') || ct.includes('application/csv') || name.endsWith('.csv')) return 'csv';
  if (ct.includes('spreadsheetml') || name.endsWith('.xlsx')) return 'xlsx';
  if (ct.includes('ms-excel') || name.endsWith('.xls')) return 'xls';
  if (ct.includes('application/xml') || ct.includes('text/xml') || name.endsWith('.xml')) return 'xml';
  return 'unknown';
}

function garantirTamanhoSeguro(buffer, maxBytes = LIMITES.MAX_BYTES) {
  if (!buffer || !buffer.length) return;
  if (buffer.length > maxBytes) {
    const e = new Error(`Arquivo maior que o permitido (${buffer.length} bytes > ${maxBytes}).`);
    e.statusCode = 413;
    throw e;
  }
}

async function parseFileToObjects({ buffer, contentType, filename, headers }) {
  if (!buffer || !buffer.length) return [];
  garantirTamanhoSeguro(buffer);

  const tipoDetectado = detectKind({ contentType, filename });
  const contexto = { buffer, headers, limites: LIMITES };

  if (tipoDetectado === 'xls') {
    const e = new Error('Arquivos .xls não são suportados por segurança. Exporte como .xlsx ou .csv.');
    e.statusCode = 400;
    throw e;
  }

  if (IMPORTADORES[tipoDetectado]) {
    return IMPORTADORES[tipoDetectado].carregar(contexto);
  }

  try {
    return await xlsxImporter.carregar(contexto);
  } catch {
    return csvImporter.carregar(contexto);
  }
}

module.exports = {
  parseFileToObjects,
  detectKind,
  constants: LIMITES,
};

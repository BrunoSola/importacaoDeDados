// src/utils/fileParser.js
// Suporta .xlsx (exceljs) e .csv. Para .xls, rejeita com instrução de conversão.
// 1ª linha = cabeçalho (case-sensitive, sem alterações). Limites para segurança.

const ExcelJS = require('exceljs');
const { parseCsvToObjects } = require('./csv');
const { parseXmlToObjects } = require('./xml');
const iconv = require('iconv-lite');

const MAX_BYTES = 5 * 1024 * 1024; // 5MB
const MAX_ROWS  = 10000;           // limite de linhas de dados (exclui header)
const MAX_COLS  = 200;             // limite de colunas

function detectKind({ contentType = '', filename = '' }) {
  const ct = (contentType || '').toLowerCase();
  const name = (filename || '').toLowerCase();

  if (ct.includes('text/csv') || ct.includes('application/csv') || name.endsWith('.csv')) return 'csv';
  if (ct.includes('spreadsheetml') || name.endsWith('.xlsx')) return 'xlsx';
  if (ct.includes('ms-excel') || name.endsWith('.xls')) return 'xls';
  if (ct.includes('application/xml') || ct.includes('text/xml') || name.endsWith('.xml')) return 'xml';
  return 'unknown';
}

/**
 * Valida o cabeçalho (1ª linha).
 * - Exige nomes não vazios
 * - Proíbe duplicados (case-sensitive)
 * - Aplica limite de colunas
 */
function validateHeader(headerArr) {
  if (!headerArr || !headerArr.length) {
    const e = new Error('Cabeçalho ausente.');
    e.statusCode = 400; throw e;
  }
  if (headerArr.some(h => !h || String(h).trim() === '')) {
    const e = new Error('Cabeçalho inválido: há coluna sem nome.');
    e.statusCode = 400; throw e;
  }
  const set = new Set(headerArr);
  if (set.size !== headerArr.length) {
    const e = new Error('Cabeçalho inválido: há colunas duplicadas.');
    e.statusCode = 400; throw e;
  }
  if (headerArr.length > MAX_COLS) {
    const e = new Error(`Arquivo com colunas demais (${headerArr.length} > ${MAX_COLS}).`);
    e.statusCode = 400; throw e;
  }
}

/**
 * Converte o valor ExcelJS em tipo primitivo JS utilizável.
 * - Preserva number/boolean
 * - Strings são trimadas
 * - Date → ISO 8601
 * - Fórmulas → usa "result" quando existir
 * - RichText/text → extrai .text
 */
function cellToPrimitive(v) {
  if (v == null) return '';
  if (v instanceof Date) return v.toISOString();

  if (typeof v === 'object') {
    // Fórmula com resultado
    if (Object.prototype.hasOwnProperty.call(v, 'result') && v.result != null) {
      return cellToPrimitive(v.result);
    }
    // RichText ou inline string
    if (Object.prototype.hasOwnProperty.call(v, 'text') && v.text != null) {
      return String(v.text).trim();
    }
    // Hyperlink (ExcelJS: { text, hyperlink })
    if (Object.prototype.hasOwnProperty.call(v, 'hyperlink')) {
      if (v.text != null) return String(v.text).trim();
      if (v.hyperlink != null) return String(v.hyperlink).trim();
      return '';
    }
    // RichText array: { richText: [{text:""}, ...] }
    if (Array.isArray(v.richText)) {
      return v.richText.map(p => p.text || '').join('').trim();
    }
    // Data serializada como objeto (casos raros)
    if (v instanceof Date) return v.toISOString();
    return '';
  }

  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return v;
  return String(v).trim();
}

/**
 * Extrai o cabeçalho da planilha (linha 1), respeitando MAX_COLS.
 */
function extractHeader(ws) {
  const headerRow = ws.getRow(1);
  const header = [];
  // Usa includeEmpty:true para não "pular" células vazias entre colunas
  headerRow.eachCell({ includeEmpty: true }, (cell, colNumber) => {
    if (colNumber > MAX_COLS) return; // aplica limite
    header.push(cellToPrimitive(cell.value));
  });

  // Remove colunas vazias no final (cauda de vazios)
  while (header.length && (header[header.length - 1] === '' || header[header.length - 1] == null)) {
    header.pop();
  }
  validateHeader(header);
  return header;
}

/**
 * Verifica se a linha é totalmente vazia, considerando N colunas do cabeçalho.
 */
function isRowEmpty(ws, rowNumber, headerLen) {
  for (let c = 1; c <= headerLen; c++) {
    const v = cellToPrimitive(ws.getCell(rowNumber, c).value);
    if (!(v === '' || v == null)) return false;
  }
  return true;
}

/**
 * Lê um XLSX a partir de Buffer e retorna array de objetos.
 * - 1ª linha = cabeçalho
 * - Limita linhas e colunas
 */
async function parseXlsxBufferToObjects(buffer, { dateAsISO = true } = {}) {
  // XLSX é um ZIP → deve começar com 'PK' (0x50 0x4B)
  if (!(buffer && buffer.length >= 2 && buffer[0] === 0x50 && buffer[1] === 0x4B)) {
    const head = buffer?.slice(0, 8)?.toString('hex') || '';
    const e = new Error(`Arquivo XLSX inválido (assinatura ZIP ausente). First bytes: ${head}`);
    e.statusCode = 400;
    throw e;
  }

  const wb = new ExcelJS.Workbook();
  try {
    await wb.xlsx.load(buffer);
  } catch (err) {
    const e = new Error(`Falha ao carregar XLSX: ${err.message || 'arquivo inválido'}`);
    e.statusCode = 400; throw e;
  }

  const ws = wb.worksheets[0];
  if (!ws) return [];

  // Cabeçalho
  const header = extractHeader(ws);

  const out = [];
  const maxRowToRead = Math.min(ws.rowCount, MAX_ROWS + 1); // +1 por causa do header

  for (let r = 2; r <= maxRowToRead; r++) {
    // Ignora linhas totalmente vazias
    if (isRowEmpty(ws, r, header.length)) continue;

    const obj = {};
    for (let c = 1; c <= header.length; c++) {
      let v = ws.getCell(r, c).value;
      let prim = cellToPrimitive(v);

      // Datas em formato número (data serial do Excel) podem vir como number.
      // O exceljs normalmente já converte para Date quando a célula é formatada como data.
      // Se quiser tentar converter números como data, implementar aqui (opcional).
      // if (typeof prim === 'number' && tratarComoData) { ... }

      // Garante ISO para Date se dateAsISO = true (cellToPrimitive já converte Date -> ISO)
      if (!dateAsISO && v instanceof Date) {
        prim = v; // devolve Date
      }

      obj[header[c - 1]] = prim;
    }
    out.push(obj);
  }

  return out;
}

function assertSize(buffer) {
  if (!buffer || !buffer.length) return;
  if (buffer.length > MAX_BYTES) {
    const e = new Error(`Arquivo maior que o permitido (${buffer.length} bytes > ${MAX_BYTES}).`);
    e.statusCode = 413; throw e;
  }
}

/** Decodificação inteligente para CSV (UTF-8 → fallback Latin1/Windows-1252) */
function decodeSmartCsv(buffer) {
  if (!buffer || !buffer.length) return '';
  let text = buffer.toString('utf8');

  // Se aparecer caractere de substituição � ou muitos bytes > 0x7F, tenta latin1
  const looksBroken = text.includes('�') || /[\x80-\x9F]/.test(text);
  if (looksBroken) {
    try {
      text = iconv.decode(buffer, 'latin1');
    } catch {
      // mantém utf8 se der algo errado
    }
  }
  return text;
}

/**
 * Roteia pelo tipo detectado:
 * - CSV → parseCsvToObjects
 * - XLSX → parseXlsxBufferToObjects
 * - XLS → rejeita
 * - Unknown → tenta XLSX; se falhar, tenta CSV
 */
async function parseFileToObjects({ buffer, contentType, filename, headers }) {
  if (!buffer || !buffer.length) return [];
  assertSize(buffer);

  const kind = detectKind({ contentType, filename });

  if (kind === 'csv') {
    // >>> CSV apenas: decodificação inteligente e parser robusto
    const text = decodeSmartCsv(buffer);
    return parseCsvToObjects(text, { maxRows: MAX_ROWS, maxCols: MAX_COLS });
  }

  if (kind === 'xlsx') {
    return await parseXlsxBufferToObjects(buffer);
  }

  if (kind === 'xls') {
    const e = new Error('Arquivos .xls não são suportados por segurança. Exporte como .xlsx ou .csv.');
    e.statusCode = 400; throw e;
  }

  if (kind === 'xml') {
    const text = buffer.toString('utf8');
    return parseXmlToObjects(text, {headers, maxRows: MAX_ROWS, maxCols: MAX_COLS});
  }

  // Fallback: tenta como XLSX; se falhar, tenta CSV (com decode inteligente)
  try {
    return await parseXlsxBufferToObjects(buffer);
  } catch {
    const text = decodeSmartCsv(buffer);
    return parseCsvToObjects(text, { maxRows: MAX_ROWS, maxCols: MAX_COLS });
  }
}

module.exports = {
  parseFileToObjects,
  // Expor utilitários caso queira testes unitários mais finos
  parseXlsxBufferToObjects,
  detectKind,
  constants: { MAX_BYTES, MAX_ROWS, MAX_COLS }
};

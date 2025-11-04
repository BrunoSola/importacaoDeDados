// src/importers/xlsxImporter.js
const ExcelJS = require('exceljs');
const { Readable } = require('stream');
const LIMITES = require('./limites');

function toCell(v) {
  if (v == null) return '';
  if (v instanceof Date) return v.toISOString();
  if (typeof v === 'object') {
    if ('result' in v && v.result != null) return toCell(v.result);
    if ('text'   in v && v.text   != null) return String(v.text).trim();
    if ('hyperlink' in v) return v.text != null ? String(v.text).trim() : String(v.hyperlink).trim();
  }
  return String(v).trim();
}

function rowIsEmpty(row, maxCols) {
  const n = Math.max(1, Math.min(Number(row?.cellCount || maxCols) || maxCols, maxCols));
  for (let c = 1; c <= n; c++) {
    const val = toCell(row.getCell(c)?.value);
    if (val !== '') return false;
  }
  return true;
}

function buildHeadersFromRow(row, maxCols) {
  const n = Math.max(1, Math.min(Number(row?.cellCount || maxCols) || maxCols, maxCols));
  const headers = [];
  for (let c = 1; c <= n; c++) {
    const val = String(toCell(row.getCell(c)?.value) || '').trim();
    headers.push(val || `col_${c}`);
  }
  return headers;
}

// ========== STREAMING ==========
async function carregarStream(contexto = {}) {
  const {
    buffer,
    limitarLinhas = 0,
    limitarColunas = LIMITES.MAX_COLS,
    planilhaIndice = 1,
  } = contexto;

  if (!buffer || !buffer.length) return [];

  const maxRows = Number(limitarLinhas) > 0 ? Number(limitarLinhas) : LIMITES.MAX_ROWS;
  const maxCols = Math.max(1, Math.min(Number(limitarColunas) || LIMITES.MAX_COLS, LIMITES.MAX_COLS));

  const linhas = [];
  const source = Readable.from(buffer);

  const reader = new ExcelJS.stream.xlsx.WorkbookReader(source, {
    entries: 'emit',
    sharedStrings: 'cache',
    styles: 'cache',
    hyperlinks: 'emit',
    worksheets: 'emit',
  });

  let sheetIdx = 0;
  let header = null; // só define quando achar a primeira linha NÃO vazia

  for await (const ws of reader) {
    if (ws.type !== 'worksheet') continue;
    sheetIdx += 1;
    if (sheetIdx !== planilhaIndice) { await ws.skip(); continue; }

    for await (const row of ws) {
      // pula linhas totalmente vazias até achar cabeçalho
      if (!header) {
        if (rowIsEmpty(row, maxCols)) continue;
        header = buildHeadersFromRow(row, maxCols);
        continue;
      }

      const cCount = Math.min(Number(row.cellCount || header.length) || header.length, maxCols);
      const rec = {};
      let hasValue = false;

      for (let c = 1; c <= cCount; c++) {
        const head = header[c - 1] || `col_${c}`;
        const val = toCell(row.getCell(c)?.value);
        if (val !== '') hasValue = true;
        rec[head] = val;
      }

      // evita empurrar linha completamente vazia
      if (hasValue) {
        linhas.push(rec);
        if (linhas.length >= maxRows) {
          if (typeof ws.abort === 'function') try { ws.abort(); } catch {}
          if (typeof reader.abort === 'function') try { reader.abort(); } catch {}
          break;
        }
      }
    }
    break; // processa só a planilha desejada
  }

  return linhas;
}

// ========== FALLBACK IN-MEMORY ==========
async function carregarFallback(contexto = {}) {
  const {
    buffer,
    limitarLinhas = 0,
    limitarColunas = LIMITES.MAX_COLS,
    planilhaIndice = 1,
  } = contexto;

  if (!buffer || !buffer.length) return [];

  const maxRows = Number(limitarLinhas) > 0 ? Number(limitarLinhas) : LIMITES.MAX_ROWS;
  const maxCols = Math.max(1, Math.min(Number(limitarColunas) || LIMITES.MAX_COLS, LIMITES.MAX_COLS));

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);

  const sheet = wb.worksheets[(planilhaIndice - 1)] || wb.worksheets[0];
  if (!sheet) return [];

  // encontra a primeira linha não vazia como cabeçalho
  let headerRowIdx = 1;
  while (headerRowIdx <= (sheet.actualRowCount || sheet.rowCount || 1)) {
    const candidate = sheet.getRow(headerRowIdx);
    if (!rowIsEmpty(candidate, maxCols)) break;
    headerRowIdx++;
  }

  const headerRow = sheet.getRow(headerRowIdx);
  const header = buildHeadersFromRow(headerRow, maxCols);

  const linhas = [];
  const lastRow = sheet.actualRowCount || sheet.rowCount || headerRowIdx;

  for (let r = headerRowIdx + 1; r <= lastRow; r++) {
    const row = sheet.getRow(r);
    const cCount = Math.min(Number(row.cellCount || header.length) || header.length, maxCols);
    const rec = {};
    let hasValue = false;

    for (let c = 1; c <= cCount; c++) {
      const head = header[c - 1] || `col_${c}`;
      const val = toCell(row.getCell(c)?.value);
      if (val !== '') hasValue = true;
      rec[head] = val;
    }

    if (hasValue) {
      linhas.push(rec);
      if (linhas.length >= maxRows) break;
    }
  }

  return linhas;
}

// ========== WRAPPER ==========
async function carregarPlanilhaXlsx(contexto = {}) {
  try {
    const viaStream = await carregarStream(contexto);
    if (viaStream && viaStream.length) return viaStream;
    // se streaming não trouxe linhas, tenta fallback
    return await carregarFallback(contexto);
  } catch {
    // se streaming falhar, tenta fallback
    return await carregarFallback(contexto);
  }
}

module.exports = {
  tipo: 'xlsx',
  carregar: carregarPlanilhaXlsx,    // wrapper (streaming + fallback)
  carregarPlanilhaXlsx,
  _stream: carregarStream,           // opcional: útil pra debug
  _fallback: carregarFallback,       // opcional
};





/*const ExcelJS = require('exceljs');
const LIMITES = require('./limites');

function valorPrimarioDaCelula(v) {
  if (v == null) return '';
  if (v instanceof Date) return v.toISOString();

  if (typeof v === 'object') {
    if (Object.prototype.hasOwnProperty.call(v, 'result') && v.result != null) {
      return valorPrimarioDaCelula(v.result);
    }
    if (Object.prototype.hasOwnProperty.call(v, 'text') && v.text != null) {
      return String(v.text).trim();
    }
    if (Object.prototype.hasOwnProperty.call(v, 'hyperlink')) {
      if (v.text != null) return String(v.text).trim();
      if (v.hyperlink != null) return String(v.hyperlink).trim();
      return '';
    }
    if (Array.isArray(v.richText)) {
      return v.richText.map(p => p.text || '').join('').trim();
    }
    if (v instanceof Date) return v.toISOString();
    return '';
  }

  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return v;
  return String(v).trim();
}

function extrairCabecalho(planilha) {
  const linhaCabecalho = planilha.getRow(1);
  const cabecalho = [];
  linhaCabecalho.eachCell({ includeEmpty: true }, (cell, colNumber) => {
    if (colNumber > LIMITES.MAX_COLS) return;
    cabecalho.push(valorPrimarioDaCelula(cell.value));
  });

  while (cabecalho.length && (cabecalho[cabecalho.length - 1] === '' || cabecalho[cabecalho.length - 1] == null)) {
    cabecalho.pop();
  }

  if (!cabecalho.length) {
    const e = new Error('Cabeçalho ausente.');
    e.statusCode = 400; throw e;
  }
  if (cabecalho.some(h => !h || String(h).trim() === '')) {
    const e = new Error('Cabeçalho inválido: há coluna sem nome.');
    e.statusCode = 400; throw e;
  }
  const set = new Set(cabecalho);
  if (set.size !== cabecalho.length) {
    const e = new Error('Cabeçalho inválido: há colunas duplicadas.');
    e.statusCode = 400; throw e;
  }
  if (cabecalho.length > LIMITES.MAX_COLS) {
    const e = new Error(`Arquivo com colunas demais (${cabecalho.length} > ${LIMITES.MAX_COLS}).`);
    e.statusCode = 400; throw e;
  }

  return cabecalho;
}

function linhaEstaVazia(planilha, numeroLinha, totalColunas) {
  for (let c = 1; c <= totalColunas; c++) {
    const valor = valorPrimarioDaCelula(planilha.getCell(numeroLinha, c).value);
    if (!(valor === '' || valor == null)) return false;
  }
  return true;
}

async function carregarPlanilhaXlsx({ buffer, limites = LIMITES, dateAsISO = true }) {
  if (!(buffer && buffer.length >= 2 && buffer[0] === 0x50 && buffer[1] === 0x4B)) {
    const head = buffer?.slice(0, 8)?.toString('hex') || '';
    const e = new Error(`Arquivo XLSX inválido (assinatura ZIP ausente). First bytes: ${head}`);
    e.statusCode = 400;
    throw e;
  }

  const workbook = new ExcelJS.Workbook();
  try {
    await workbook.xlsx.load(buffer);
  } catch (err) {
    const e = new Error(`Falha ao carregar XLSX: ${err.message || 'arquivo inválido'}`);
    e.statusCode = 400; throw e;
  }

  const planilha = workbook.worksheets[0];
  if (!planilha) return [];

  const cabecalho = extrairCabecalho(planilha);
  const linhas = [];
  const ultimaLinha = Math.min(planilha.rowCount, limites.MAX_ROWS + 1);

  for (let linha = 2; linha <= ultimaLinha; linha++) {
    if (linhaEstaVazia(planilha, linha, cabecalho.length)) continue;

    const registro = {};
    for (let col = 1; col <= cabecalho.length; col++) {
      let valor = planilha.getCell(linha, col).value;
      let primario = valorPrimarioDaCelula(valor);
      if (!dateAsISO && valor instanceof Date) {
        primario = valor;
      }
      registro[cabecalho[col - 1]] = primario;
    }
    linhas.push(registro);
  }

  return linhas;
}

module.exports = {
  tipo: 'xlsx',
  carregar: carregarPlanilhaXlsx,
  carregarPlanilhaXlsx,
};*/

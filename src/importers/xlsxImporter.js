// src/importers/xlsxImporter.js
// Importador de XLSX com foco em baixo uso de memória/CPU na Lambda.
// Mantém o contrato atual (exporta { tipo, carregar, carregarPlanilhaXlsx }) e
// adiciona leitura por FAIXA (carregarFaixa) usando streaming quando possível.

const ExcelJS = require('exceljs');
const { Readable } = require('stream');
const LIMITES = require('./limites');

/* ========================================================================
 * Conversão de célula → valor serializável
 * - Objetivo: padronizar saídas e evitar objetos internos do ExcelJS no JSON.
 * - Mantém compatibilidade retornando strings (como seu importador atual faz).
 * ======================================================================== */
function toCell(cellRawValue) {
  if (cellRawValue == null) return '';
  if (cellRawValue instanceof Date) return cellRawValue.toISOString();

  if (typeof cellRawValue === 'object') {
    if ('result' in cellRawValue && cellRawValue.result != null) return toCell(cellRawValue.result);
    if ('text' in cellRawValue && cellRawValue.text != null) return String(cellRawValue.text).trim();
    if ('hyperlink' in cellRawValue) {
      return cellRawValue.text != null ? String(cellRawValue.text).trim() : String(cellRawValue.hyperlink).trim();
    }
    if (Array.isArray(cellRawValue.richText)) {
      return cellRawValue.richText.map(p => p.text || '').join('').trim();
    }
  }

  return String(cellRawValue).trim();
}

/* ========================================================================
 * Linha vazia?
 * - Objetivo: pular “linhas zumbis” (formatadas, porém sem dados).
 * - maxCols limita o scan por segurança (colunas demais = custo desnecessário).
 * ======================================================================== */
function rowIsEmpty(row, maxCols) {
  const totalColsToCheck = Math.max(1, Math.min(Number(row?.cellCount || maxCols) || maxCols, maxCols));
  for (let c = 1; c <= totalColsToCheck; c++) {
    const val = toCell(row.getCell(c)?.value);
    if (val !== '') return false;
  }
  return true;
}

/* ========================================================================
 * Cabeçalho a partir da primeira linha não-vazia
 * - Objetivo: manter seu padrão de “primeira linha com dados = header”.
 * - Garante nomes válidos, preenchendo “col_1”, “col_2”, ... se vazio.
 * ======================================================================== */
function buildHeadersFromRow(row, maxCols) {
  const totalCols = Math.max(1, Math.min(Number(row?.cellCount || maxCols) || maxCols, maxCols));
  const headers = [];
  for (let c = 1; c <= totalCols; c++) {
    const headerName = String(toCell(row.getCell(c)?.value) || '').trim();
    headers.push(headerName || `col_${c}`);
  }
  return headers;
}

function materializarRow(row, header, maxCols) {
  const cellCount = Math.min(Number(row?.cellCount || header.length) || header.length, maxCols);
  const record = {};
  let hasValue = false;

  for (let c = 1; c <= cellCount; c++) {
    const key = header[c - 1] || `col_${c}`;
    const val = toCell(row.getCell(c)?.value);
    if (val !== '') hasValue = true;
    record[key] = val;
  }

  return { record, hasValue };
}

/* ========================================================================
 * Leitura via STREAMING (preferencial)
 * - Objetivo: reduzir picos de memória (não materializa a planilha inteira).
 * - Para imediatamente ao atingir LIMITES.MAX_ROWS (ou limitarLinhas).
 * - Processa apenas a primeira worksheet (padrão do projeto) ou a desejada.
 * ======================================================================== */
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

  const outputRows = [];
  const source = Readable.from(buffer);

  const reader = new ExcelJS.stream.xlsx.WorkbookReader(source, {
    entries: 'emit',
    sharedStrings: 'cache',
    styles: 'cache',
    hyperlinks: 'emit',
    worksheets: 'emit',
  });

  let currentSheetIndex = 0;
  let header = null;

  for await (const worksheet of reader) {
    if (worksheet.type !== 'worksheet') continue;
    currentSheetIndex += 1;

    if (currentSheetIndex !== planilhaIndice) {
      await worksheet.skip();
      continue;
    }

    for await (const row of worksheet) {
      // Descobrir cabeçalho
      if (!header) {
        if (rowIsEmpty(row, maxCols)) continue;
        header = buildHeadersFromRow(row, maxCols);
        continue;
      }

      // Linhas de dados
      const { record, hasValue } = materializarRow(row, header, maxCols);

      if (hasValue) {
        outputRows.push(record);
        if (outputRows.length >= maxRows) {
          if (typeof worksheet.abort === 'function') try { worksheet.abort(); } catch {}
          if (typeof reader.abort === 'function') try { reader.abort(); } catch {}
          break;
        }
      }
    }
    break; // só a planilha alvo
  }

  return outputRows;
}

/* ========================================================================
 * Fallback in-memory (Workbook completo)
 * - Objetivo: compatibilidade para cenários onde o streaming não atende.
 * - Ainda respeita LIMITES.MAX_ROWS e LIMITES.MAX_COLS.
 * ======================================================================== */
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

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);

  const sheet = workbook.worksheets[(planilhaIndice - 1)] || workbook.worksheets[0];
  if (!sheet) return [];

  // Achar a primeira linha não-vazia para virar cabeçalho
  let headerRowIndex = 1;
  const lastRowIndex = sheet.actualRowCount || sheet.rowCount || 1;

  while (headerRowIndex <= lastRowIndex) {
    const candidate = sheet.getRow(headerRowIndex);
    if (!rowIsEmpty(candidate, maxCols)) break;
    headerRowIndex++;
  }

  const headerRow = sheet.getRow(headerRowIndex);
  const header = buildHeadersFromRow(headerRow, maxCols);

  const outputRows = [];
  const dataLastIndex = sheet.actualRowCount || sheet.rowCount || headerRowIndex;

  for (let r = headerRowIndex + 1; r <= dataLastIndex; r++) {
    const row = sheet.getRow(r);
    const { record, hasValue } = materializarRow(row, header, maxCols);

    if (hasValue) {
      outputRows.push(record);
      if (outputRows.length >= maxRows) break;
    }
  }

  return outputRows;
}

/* ========================================================================
 * Wrapper público: carregarPlanilhaXlsx
 * - Objetivo: preservar sua assinatura/export atual (usado por outros módulos).
 * - Estratégia: tenta streaming; se vazio/falhar, cai para fallback.
 * ======================================================================== */
async function carregarPlanilhaXlsx(contexto = {}) {
  try {
    const viaStreaming = await carregarStream(contexto);
    if (viaStreaming && viaStreaming.length) return viaStreaming;
    return await carregarFallback(contexto);
  } catch {
    return await carregarFallback(contexto);
  }
}

/* ========================================================================
 * NOVO: Leitura por FAIXA (offset/limit) — ideal p/ x-offset/x-batch-size
 * - Objetivo: montar só a janela a ser enviada agora (economia de CPU/memória).
 * - Implementação: streaming preferencial; fallback fatia do resultado completo.
 * - offset é 0-based sobre as LINHAS DE DADOS (após o cabeçalho).
 * ======================================================================== */
async function carregarFaixa(contexto = {}) {
  const {
    buffer,
    offset = 0,
    limit = 0,
    limitarColunas = LIMITES.MAX_COLS,
    planilhaIndice = 1,
  } = contexto;

  if (!buffer || !buffer.length) return [];

  const safeOffset = Math.max(0, Number(offset || 0));
  const safeLimit = Math.max(0, Number(limit || 0));
  const wantsLimit = safeLimit > 0;
  const maxCols = Math.max(1, Math.min(Number(limitarColunas) || LIMITES.MAX_COLS, LIMITES.MAX_COLS));

  // --- Streaming preferencial: pula até o offset sem materializar tudo ---
  try {
    const source = Readable.from(buffer);
    const reader = new ExcelJS.stream.xlsx.WorkbookReader(source, {
      entries: 'emit',
      sharedStrings: 'cache',
      styles: 'cache',
      hyperlinks: 'emit',
      worksheets: 'emit',
    });

    let currentSheetIndex = 0;
    let header = null;
    let produced = 0;
    let skippedDataRows = 0;
    const windowRows = [];

    for await (const worksheet of reader) {
      if (worksheet.type !== 'worksheet') continue;
      currentSheetIndex += 1;
      if (currentSheetIndex !== planilhaIndice) {
        await worksheet.skip();
        continue;
      }

      for await (const row of worksheet) {
        // Detectar cabeçalho
        if (!header) {
          if (rowIsEmpty(row, maxCols)) continue;
          header = buildHeadersFromRow(row, maxCols);
          continue;
        }

        // Pular até o início da janela
        if (skippedDataRows < safeOffset) {
          if (!rowIsEmpty(row, maxCols)) skippedDataRows++;
          continue;
        }

        // Coletar dentro da janela
        const { record, hasValue } = materializarRow(row, header, maxCols);

        if (hasValue) {
          windowRows.push(record);
          produced++;
          if (wantsLimit && produced >= safeLimit) {
            if (typeof worksheet.abort === 'function') try { worksheet.abort(); } catch {}
            if (typeof reader.abort === 'function') try { reader.abort(); } catch {}
            break;
          }
        }
      }

      break; // processa apenas a planilha alvo
    }

    return windowRows;
  } catch {
    // --- Fallback universal: lê tudo e fatia (correto, porém mais custoso) ---
    const all = await carregarPlanilhaXlsx({ buffer, limitarColunas, planilhaIndice });
    const endIndex = wantsLimit ? safeOffset + safeLimit : all.length;
    return all.slice(safeOffset, endIndex);
  }
}

/* ========================================================================
 * Exports estáveis (compat) + novas capacidades
 * - Não afeta quem já faz require('xlsxImporter').carregar(...)
 * - Adiciona carregarFaixa para uso via fileParser.parseFileToObjectsRange(...)
 * ======================================================================== */
module.exports = {
  tipo: 'xlsx',
  carregar: carregarPlanilhaXlsx,  // mantém contrato atual
  carregarPlanilhaXlsx,            // alias explícito
  carregarFaixa,                   // NOVO: leitura por janela
  _stream: carregarStream,         // opcional (debug)
  _fallback: carregarFallback,     // opcional (debug)
};

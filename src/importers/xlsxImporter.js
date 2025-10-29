const ExcelJS = require('exceljs');
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
};

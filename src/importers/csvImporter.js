const iconv = require('iconv-lite');
const LIMITES = require('./limites');
const { parseCsvToObjects } = require('../utils/csv');

function decodificarCsv(buffer) {
  if (!buffer || !buffer.length) return '';
  let texto = buffer.toString('utf8');
  const pareceQuebrado = texto.includes('�') || /[\x80-\x9F]/.test(texto);
  if (pareceQuebrado) {
    try {
      texto = iconv.decode(buffer, 'latin1');
    } catch {
      // mantém utf8 se falhar
    }
  }
  return texto;
}

async function carregarTabelaCsv({ buffer, limites = LIMITES }) {
  const texto = decodificarCsv(buffer);
  return parseCsvToObjects(texto, { maxRows: limites.MAX_ROWS, maxCols: limites.MAX_COLS });
}

module.exports = {
  tipo: 'csv',
  carregar: carregarTabelaCsv,
  carregarTabelaCsv,
};

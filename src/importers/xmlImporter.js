const LIMITES = require('./limites');
const { parseXmlToObjects } = require('../utils/xml');

async function carregarXmlParaObjetos({ buffer, headers, limites = LIMITES }) {
  const texto = buffer.toString('utf8');
  return parseXmlToObjects(texto, {
    headers,
    maxRows: limites.MAX_ROWS,
    maxCols: limites.MAX_COLS,
  });
}

module.exports = {
  tipo: 'xml',
  carregar: carregarXmlParaObjetos,
  carregarXmlParaObjetos,
};

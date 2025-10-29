const { normalizarDataHora, normalizarBooleano } = require('./normalizadores');

function limparRegistroPlano(registro) {
  const resultado = {};
  for (const [chave, valor] of Object.entries(registro || {})) {
    let atual = typeof valor === 'string' ? valor.trim() : (valor ?? '');
    if (typeof atual === 'string') {
      atual = normalizarDataHora(atual);
      atual = normalizarBooleano(atual);
    } else {
      atual = normalizarBooleano(atual);
    }
    resultado[chave] = atual;
  }
  return resultado;
}

module.exports = {
  limparRegistroPlano,
};

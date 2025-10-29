async function mapearComLimite(itens, limite, trabalhador) {
  const resultados = new Array(itens.length);
  let proximoIndice = 0;

  async function executar() {
    while (true) {
      const indiceAtual = proximoIndice++;
      if (indiceAtual >= itens.length) return;
      resultados[indiceAtual] = await trabalhador(itens[indiceAtual], indiceAtual);
    }
  }

  const execucoes = [];
  for (let k = 0; k < Math.min(limite, itens.length); k++) {
    execucoes.push(executar());
  }
  await Promise.all(execucoes);
  return resultados;
}

module.exports = {
  mapearComLimite,
};

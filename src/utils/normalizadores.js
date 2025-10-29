function mediaNumeros(numeros = []) {
  if (!numeros.length) return 0;
  const soma = numeros.reduce((acc, valor) => acc + valor, 0);
  return Math.round(soma / numeros.length);
}

function percentil(numeros = [], percentual) {
  if (!numeros.length) return 0;
  const ordenados = [...numeros].sort((a, b) => a - b);
  const indice = Math.min(
    ordenados.length - 1,
    Math.max(0, Math.floor((percentual / 100) * ordenados.length))
  );
  return ordenados[indice];
}

function numeroFinito(valor, fallback = 0) {
  const convertido = Number(valor);
  return Number.isFinite(convertido) ? convertido : fallback;
}

function normalizarDataHora(valor) {
  if (valor == null || valor === '') return valor;
  const texto = String(valor).trim();

  let partes = texto.match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2}):(\d{2})$/);
  if (partes) return `${partes[3]}-${partes[2]}-${partes[1]} ${partes[4]}:${partes[5]}:${partes[6]}`;

  partes = texto.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (partes) return `${partes[3]}-${partes[2]}-${partes[1]} 00:00:00`;

  partes = texto.match(/^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2})$/);
  if (partes) return `${partes[3]}-${partes[2]}-${partes[1]} ${partes[4]}:${partes[5]}:00`;

  partes = texto.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (partes) return `${partes[1]}-${partes[2]}-${partes[3]} 00:00:00`;

  partes = texto.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/);
  if (partes) return `${partes[1]}-${partes[2]}-${partes[3]} ${partes[4]}:${partes[5]}:00`;

  partes = texto.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$/);
  if (partes) return `${partes[1]}-${partes[2]}-${partes[3]} ${partes[4]}:${partes[5]}:${partes[6]}`;

  partes = texto.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})$/);
  if (partes) return `${partes[1]}-${partes[2]}-${partes[3]} ${partes[4]}:${partes[5]}:00`;

  return texto;
}

function normalizarBooleano(valor) {
  if (valor == null) return valor;
  if (typeof valor === 'boolean') return valor;

  if (typeof valor === 'string') {
    const texto = valor.trim().toLowerCase();
    if (['true', '1', 'yes', 'y', 'sim', 's'].includes(texto)) return true;
    if (['false', '0', 'no', 'n', 'nao', 'não'].includes(texto)) return false;
  }

  if (typeof valor === 'number') {
    if (valor === 1) return true;
    if (valor === 0) return false;
  }

  return valor;
}

module.exports = {
  mediaNumeros,
  percentil,
  numeroFinito,
  normalizarDataHora,
  normalizarBooleano,
};

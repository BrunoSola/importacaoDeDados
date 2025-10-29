const { normalizarBooleano } = require('./normalizadores');

function numeroPorLocale(entrada, tipo = 'float') {
  if (entrada == null || entrada === '') return entrada;
  if (typeof entrada === 'number') return entrada;
  let texto = String(entrada).trim();
  const ultimaVirgula = texto.lastIndexOf(',');
  const ultimoPonto = texto.lastIndexOf('.');
  if (/[.,]/.test(texto)) {
    if (ultimaVirgula > ultimoPonto) texto = texto.replace(/\./g, '').replace(/,/g, '.');
    else texto = texto.replace(/,/g, '.');
  }
  const numero = tipo === 'int' ? parseInt(texto, 10) : parseFloat(texto);
  return Number.isFinite(numero) ? numero : entrada;
}

function ajustarValor(valor, tipo) {
  if (!tipo) return (typeof valor === 'string' ? valor.trim() : valor);
  const tipoNormalizado = String(tipo).toLowerCase();
  switch (tipoNormalizado) {
    case 'int':
    case 'integer':
      return numeroPorLocale(valor, 'int');
    case 'float':
    case 'number':
    case 'decimal':
      return numeroPorLocale(valor, 'float');
    case 'bool':
    case 'boolean':
      return normalizarBooleano(valor);
    case 'string':
      return valor == null ? valor : String(valor).trim();
    case 'date':
      return valor == null ? valor : String(valor).trim();
    default:
      return (typeof valor === 'string' ? valor.trim() : valor);
  }
}

function mapaDeTipos(headers) {
  const mapa = {};
  const bruto = headers['x-const-types'];
  if (bruto) {
    try {
      if (typeof bruto === 'string' && bruto.trim().startsWith('{')) {
        const obj = JSON.parse(bruto);
        for (const [chave, tipo] of Object.entries(obj)) mapa[String(chave).trim()] = String(tipo).toLowerCase();
      } else {
        String(bruto)
          .split(',')
          .forEach(par => {
            const [chave, tipo] = par.split(':').map(s => s.trim()).filter(Boolean);
            if (chave && tipo) mapa[chave] = tipo.toLowerCase();
          });
      }
    } catch {
      // ignora json inválido
    }
  }

  const adicionarLista = (header, tipo) => {
    const valor = headers[header];
    if (!valor) return;
    String(valor)
      .split(',')
      .map(s => s.trim())
      .filter(Boolean)
      .forEach(chave => (mapa[chave] = tipo));
  };

  adicionarLista('x-const-int', 'int');
  adicionarLista('x-const-float', 'float');
  adicionarLista('x-const-bool', 'bool');
  adicionarLista('x-const-string', 'string');

  return mapa;
}

function chaveTipada(headerKey) {
  const correspondencia = String(headerKey)
    .match(/^x-const-(.+?)(?:[_.](int|integer|float|number|decimal|bool|boolean|string|date))$/i);
  return correspondencia
    ? { campo: correspondencia[1], tipo: correspondencia[2].toLowerCase() }
    : null;
}

function extrairConstantes(headersLower = {}, headersRaw = {}) {
  const constantes = {};
  const tipos = mapaDeTipos(headersLower);

  const indiceOriginal = {};
  for (const chaveOriginal of Object.keys(headersRaw || {})) {
    indiceOriginal[String(chaveOriginal).toLowerCase()] = chaveOriginal;
  }

  for (const [chaveLower, valor] of Object.entries(headersLower || {})) {
    if (!chaveLower.startsWith('x-const-')) continue;
    if ([
      'x-const',
      'x-consts',
      'x-const-types',
      'x-const-int',
      'x-const-float',
      'x-const-bool',
      'x-const-string',
    ].includes(chaveLower)) continue;

    const chaveOriginal = indiceOriginal[chaveLower] || chaveLower;
    let campo = chaveOriginal.slice('x-const-'.length);
    let tipoExplicito = tipos[campo] || tipos[campo?.toLowerCase()];

    const tipado = chaveTipada(chaveOriginal);
    if (tipado) {
      campo = tipado.campo;
      tipoExplicito = tipado.tipo || tipoExplicito;
    }

    if (valor !== undefined && valor !== null && String(valor).trim() !== '') {
      constantes[campo] = ajustarValor(valor, tipoExplicito);
    }
  }

  const jsonConst = headersLower['x-const'] || headersLower['x-consts'];
  if (jsonConst) {
    try {
      const parsed = typeof jsonConst === 'string' ? JSON.parse(jsonConst) : jsonConst;
      if (parsed && typeof parsed === 'object') {
        for (const [chave, valor] of Object.entries(parsed)) {
          constantes[chave] = tipos[chave] ? ajustarValor(valor, tipos[chave]) : valor;
        }
      }
    } catch {
      // JSON inválido – ignora
    }
  }

  return constantes;
}

function aplicarConstantes(registro, constantes, sobrescrever = false) {
  const saida = { ...registro };
  for (const [chave, valor] of Object.entries(constantes || {})) {
    const atual = saida[chave];
    const vazio = atual === undefined || atual === null || String(atual).trim() === '';
    if (sobrescrever || vazio) {
      saida[chave] = typeof valor === 'string' ? valor.trim() : valor;
    }
  }
  return saida;
}

module.exports = {
  extrairConstantes,
  aplicarConstantes,
  ajustarValor,
  numeroPorLocale,
};

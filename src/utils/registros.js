// src/utils/registros.js

const { normalizarDataHora } = require('./normalizadores');

/**
 * Converte valores de forma segura:
 * - '' | null | undefined => undefined
 * - 'true'/'false' (case-insensitive) => boolean
 * - números em string (com vírgula/.) => Number
 * - datas => usa normalizarDataHora (mantém se não for data válida)
 * - arrays/objetos => aplica recursivamente e remove undefined
 */
function normalizarValor(v) {
  // 1) vazios
  if (v === '' || v === null || v === undefined) return undefined;

  // 2) strings
  if (typeof v === 'string') {
    const s = v.trim();
    if (s === '') return undefined;

    // tenta data (usa sua função; se não mudar, mantém s)
    const sData = normalizarDataHora(s);
    if (sData !== s) return sData;

    // booleans explícitos
    const sl = s.toLowerCase();
    if (sl === 'true') return true;
    if (sl === 'false') return false;

    // números: remove milhares ".", troca vírgula por ponto
    const numStr = s.replace(/\./g, '').replace(',', '.');
    if (/^-?\d+(\.\d+)?$/.test(numStr)) return Number(numStr);

    // mantém string original
    return s;
  }

  // 3) arrays
  if (Array.isArray(v)) {
    const arr = v.map(normalizarValor).filter((x) => x !== undefined);
    return arr;
  }

  // 4) objetos
  if (typeof v === 'object') {
    const o = {};
    for (const [k, val] of Object.entries(v)) {
      const nv = normalizarValor(val);
      if (nv !== undefined) o[k] = nv;
    }
    return o;
  }

  // 5) tipos primitivos já corretos (number/boolean)
  return v;
}

function limparRegistroPlano(registro) {
  // aplica normalização recursiva e remove chaves indefinidas
  return normalizarValor(registro);
}

module.exports = { limparRegistroPlano };

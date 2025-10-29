// src/utils/xml.js
// XML → Array<Object> com suporte a:
// - x-xml-record-path: caminho dos nós "linha" (obrigatório)
// - x-xml-map: JSON { campoDestino: "caminho.origem" } (opcional; tenta relativo ao nó e depois absoluto)
// - x-xml-number: lista de campos destino numéricos (ex.: "qtd,valor_total")
// - x-xml-date: lista de campos destino tratados como data (normalização ocorre depois no sanitize)
// Limites: maxRows e maxCols.

const { XMLParser } = require('fast-xml-parser');

function toLowerHeaders(h = {}) {
  return Object.fromEntries(Object.entries(h).map(([k, v]) => [String(k).toLowerCase(), v]));
}

function parseListHeader(v) {
  if (!v) return [];
  return String(v)
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

function parseJsonMap(v) {
  if (!v) return null;
  try {
    return typeof v === 'string' ? JSON.parse(v) : v;
  } catch {
    const e = new Error('x-xml-map inválido: JSON malformado.');
    e.statusCode = 400; throw e;
  }
}

function toNumberLocale(input) {
  if (input == null || input === '') return input;
  if (typeof input === 'number') return input;
  let s = String(input).trim();
  const lastComma = s.lastIndexOf(',');
  const lastDot = s.lastIndexOf('.');
  if (/[.,]/.test(s)) {
    if (lastComma > lastDot) s = s.replace(/\./g, '').replace(/,/g, '.');
    else s = s.replace(/,/g, '.');
  }
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : input;
}

function selectNodes(root, path) {
  // Caminho com pontos. Suporta arrays implicitamente.
  // Ex.: "NFe.infNFe.det"
  if (!path) return [];
  const parts = String(path).split('.').filter(Boolean);

  let level = [root];
  for (const p of parts) {
    const next = [];
    for (const node of level) {
      const v = node?.[p];
      if (Array.isArray(v)) next.push(...v);
      else if (v !== undefined && v !== null) next.push(v);
    }
    level = next;
    if (!level.length) break;
  }
  // Garante array de objetos/valores
  return Array.isArray(level) ? level : [level];
}

function pickByPath(root, base, path) {
  if (!path) return undefined;
  // 1) tenta relativo ao nó base
  let val = _pick(base, path);
  if (val === undefined) {
    // 2) tenta absoluto a partir do root
    val = _pick(root, path);
  }
  return val;
}

function _pick(obj, path) {
  if (!obj) return undefined;
  const parts = String(path).split('.').filter(Boolean);
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function flatten(obj, prefix = '', out = {}, maxCols = 200) {
  if (out && Object.keys(out).length >= maxCols) return out;
  if (obj == null) return out;

  if (typeof obj !== 'object') {
    const key = prefix || 'value';
    if (!(key in out)) out[key] = obj;
    return out;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      flatten(obj[i], prefix ? `${prefix}[${i}]` : `[${i}]`, out, maxCols);
      if (Object.keys(out).length >= maxCols) break;
    }
    return out;
  }

  for (const [k, v] of Object.entries(obj)) {
    const next = prefix ? `${prefix}.${k}` : k;
    if (typeof v === 'object' && v !== null) {
      flatten(v, next, out, maxCols);
    } else {
      if (!(next in out)) out[next] = v;
    }
    if (Object.keys(out).length >= maxCols) break;
  }
  return out;
}

/**
 * @param {string} xmlText
 * @param {object} opts { headers, maxRows, maxCols }
 * @returns {Promise<Array<Object>>}
 */
async function parseXmlToObjects(xmlText, { headers = {}, maxRows = 10000, maxCols = 200 } = {}) {
  const h = toLowerHeaders(headers);
  const recordPath = h['x-xml-record-path'];
  if (!recordPath) {
    const e = new Error('Header "x-xml-record-path" é obrigatório para XML.');
    e.statusCode = 400; throw e;
  }

  const map = parseJsonMap(h['x-xml-map']);
  const numberKeys = parseListHeader(h['x-xml-number']);
  const dateKeys = parseListHeader(h['x-xml-date']); // normalização posterior no sanitize

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@',     // atributos como "@Id"
    allowBooleanAttributes: true,
    trimValues: true,
    parseTagValue: false,         // mantém strings; números/datas tratamos depois
    parseAttributeValue: false,
  });

  let doc;
  try {
    doc = parser.parse(xmlText || '');
  } catch (err) {
    const e = new Error(`XML inválido: ${err.message || 'falha ao parsear'}`);
    e.statusCode = 400; throw e;
  }

  const nodes = selectNodes(doc, recordPath);
  if (!nodes.length) {
    const e = new Error(`x-xml-record-path não encontrou nós: "${recordPath}".`);
    e.statusCode = 400; throw e;
  }

  const limit = Math.min(nodes.length, maxRows);
  const out = [];

  for (let i = 0; i < limit; i++) {
    const node = nodes[i];

    let rec;
    if (map && Object.keys(map).length) {
      rec = {};
      for (const [dest, srcPath] of Object.entries(map)) {
        let val = pickByPath(doc, node, srcPath);
        // cast numérico quando aplicável
        if (numberKeys.includes(dest)) val = toNumberLocale(val);
        // datas serão normalizadas no sanitize do handler
        rec[dest] = val ?? '';
        if (Object.keys(rec).length >= maxCols) break;
      }
    } else {
      // sem map → flatten do nó
      rec = flatten(node, '', {}, maxCols);
    }

    out.push(rec);
  }

  return out;
}

module.exports = { parseXmlToObjects };

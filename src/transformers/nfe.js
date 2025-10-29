// src/importers/nfe.js
const { detectKind } = require('../utils/fileParser');

const FIXED_PARENT_GUID = '82024372-8ca7-869a-ac13-e0a3ef95396f'; // pai fixo
const CHILD_FORM_ID     = 10741;                                     // relacionamento filho

function looksLikeNFe({ contentType, filename, buffer }) {
  const isXml = detectKind({ contentType, filename }) === 'xml';
  if (!isXml) return false;
  const head = buffer?.slice(0, 32 * 1024)?.toString('utf8') || '';
  return /<(?:NFe|nfeProc)\b/i.test(head);
}

function buildNfePayload(master, itens) {
  return {
    "__recordguid__": FIXED_PARENT_GUID,
    "__relationships__": [
      {
        Id: CHILD_FORM_ID,
        childrens: [],
        records: itens.map(it => ({
          "__record_parent_guid__": FIXED_PARENT_GUID,
          "xProd": it?.xProd ?? "",
          "C99_FINANCEIRO_NOTA_FISCAL_Id": it?.C99_FINANCEIRO_NOTA_FISCAL_Id ?? ""
          // mapear demais campos do item aqui se necessário
        }))
      }
    ],
    "nfe_chave": master?.nfe_chave || master?.chNFe || master?.chave || master?.chaveNFe || ""
    // ...outros campos do pai se quiser
  };
}

function transformNfeRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return rows;

  // agrupa por chave
  const keyOf = r => String(r?.nfe_chave || r?.chNFe || r?.chave || r?.chaveNFe || '').trim();
  const groups = new Map();
  for (const r of rows) {
    const k = keyOf(r);
    if (!k) continue;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(r);
  }
  if (!groups.size) return rows;

  // monta 1 payload por chave
  const out = [];
  for (const [nfe_chave, itens] of groups.entries()) {
    const master = { nfe_chave };
    out.push(buildNfePayload(master, itens));
  }
  return out;
}

module.exports = {
  FIXED_PARENT_GUID,
  CHILD_FORM_ID,
  looksLikeNFe,
  buildNfePayload,
  transformNfeRows
};

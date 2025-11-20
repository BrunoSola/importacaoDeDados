// src/utils/registros.js
const { normalizarDataHora } = require('./normalizadores');

const REL_REGEX = /^(.*)_rel\[(\d+)\]$/;

/**
 * Converte valores de forma segura:
 * - '' | null | undefined => undefined
 * - 'true'/'false' (case-insensitive) => boolean
 * - numeros em string (com virgula/.) => Number
 * - datas => usa normalizarDataHora (mantem se nao for data valida)
 * - arrays/objetos => aplica recursivamente e remove undefined
 *
 * Usado somente para caminhos que precisam de normalizacao (template/Integracao).
 */
function normalizarValor(v) {
  if (v === '' || v === null || v === undefined) return undefined;

  if (typeof v === 'string') {
    const s = v.trim();
    if (s === '') return undefined;

    const sData = normalizarDataHora(s);
    if (sData !== s) return sData;

    const sl = s.toLowerCase();
    if (sl === 'true') return true;
    if (sl === 'false') return false;

    const numStr = s.replace(/\./g, '').replace(',', '.');
    if (/^-?\d+(\.\d+)?$/.test(numStr)) return Number(numStr);

    return s;
  }

  if (Array.isArray(v)) {
    return v.map(normalizarValor).filter((x) => x !== undefined);
  }

  if (typeof v === 'object') {
    return Object.entries(v).reduce((acc, [k, val]) => {
      const nv = normalizarValor(val);
      if (nv !== undefined) acc[k] = nv;
      return acc;
    }, {});
  }

  return v;
}

/**
 * Tenta descobrir o GUID do registro pai em diferentes convencoes.
 */
function extrairParentGuid(obj) {
  if (!obj || typeof obj !== 'object') return null;
  return obj.__recordguid__ || obj.__record_guid__ || obj.recordguid || null;
}

/**
 * Helper para dividir o valor bruto dos campos *_rel[ID].
 * Nao faz normalizacao de tipo - serve para modo DIRETO.
 */
function splitRelValorBruto(valor) {
  if (valor === undefined || valor === null || valor === '') return [];

  if (Array.isArray(valor)) {
    return valor.filter((v) => v !== undefined && v !== null && v !== '');
  }

  if (typeof valor === 'string') {
    return valor
      .split('|')
      .map((s) => s.trim())
      .filter(Boolean);
  }

  return [valor];
}

/**
 * Separa campos *_rel[ID] (relationships) dos demais campos de base.
 */
function separarBaseECamposRel(registro) {
  const base = {};
  const camposRel = [];

  for (const [chave, valor] of Object.entries(registro)) {
    const match = REL_REGEX.exec(chave);
    if (match) {
      const [, nomeCampo, idStr] = match;
      camposRel.push({ nomeCampo, idStr, valor });
      continue;
    }
    base[chave] = valor;
  }

  return { base, camposRel };
}

function construirRelationships(camposRel, parentGuid, processarValor, ignorarUndefined) {
  const porId = {};

  for (const { nomeCampo, idStr, valor } of camposRel) {
    const valoresArray = splitRelValorBruto(valor);
    if (!valoresArray.length) continue;

    const keyId = String(idStr);
    if (!porId[keyId]) {
      const idNum = Number(idStr);
      porId[keyId] = {
        Id: Number.isFinite(idNum) ? idNum : idStr,
        childrens: [],
        records: [],
      };
    }
    const rel = porId[keyId];

    valoresArray.forEach((val, idx) => {
      const valorProcessado = processarValor(val);
      if (ignorarUndefined && valorProcessado === undefined) return;

      if (!rel.records[idx]) rel.records[idx] = {};
      rel.records[idx][nomeCampo] = valorProcessado;
    });
  }

  return Object.values(porId)
    .map((rel) => {
      const records = (rel.records || [])
        .filter((r) => r && Object.keys(r).length > 0)
        .map((r) => {
          const rec = { ...r };

          // filho nao deve carregar relationships ou childrens aninhados
          if ('__relationships__' in rec) delete rec.__relationships__;
          if ('childrens' in rec) delete rec.childrens;

          if (
            parentGuid &&
            (rec.__record_parent_guid__ == null || rec.__record_parent_guid__ === '')
          ) {
            rec.__record_parent_guid__ = parentGuid;
          }
          return rec;
        });

      return {
        Id: rel.Id,
        childrens: Array.isArray(rel.childrens) ? rel.childrens : [],
        records,
      };
    })
    .filter((rel) => rel.records.length > 0);
}

/**
 * Versao COMPLETA (com normalizacao) - usada para TEMPLATE / INTEGRACAO.
 * Cabecalhos: nomeCampo_rel[10855] => relationships.
 */
function montarRelationshipsPorHeaders(registroNormalizado) {
  if (
    !registroNormalizado ||
    typeof registroNormalizado !== 'object' ||
    Array.isArray(registroNormalizado)
  ) {
    return { base: registroNormalizado, relationships: [] };
  }

  const { base, camposRel } = separarBaseECamposRel(registroNormalizado);

  if (!camposRel.length) {
    return { base, relationships: [] };
  }

  const parentGuid =
    extrairParentGuid(base) || extrairParentGuid(registroNormalizado);

  const relationships = construirRelationships(
    camposRel,
    parentGuid,
    normalizarValor,
    true
  );
  return { base, relationships };
}

/**
 * Versao LEVE (sem normalizar tipos) - usada para ENVIO DIRETO.
 * Mesma ideia de montarRelationshipsPorHeaders, so que sem normalizarValor.
 */
function montarRelationshipsPorHeadersSemNormalizar(registroOriginal) {
  if (
    !registroOriginal ||
    typeof registroOriginal !== 'object' ||
    Array.isArray(registroOriginal)
  ) {
    return { base: registroOriginal, relationships: [] };
  }

  const { base, camposRel } = separarBaseECamposRel(registroOriginal);

  if (!camposRel.length) {
    return { base, relationships: [] };
  }

  const parentGuid =
    extrairParentGuid(base) || extrairParentGuid(registroOriginal);

  const relationships = construirRelationships(
    camposRel,
    parentGuid,
    (v) => v,
    false
  );
  return { base, relationships };
}

/**
 * Mescla relacionamentos novos com existentes (se houver).
 */
function mesclarRelationships(base, origem, novosRelationships) {
  const existentes = Array.isArray(base.__relationships__)
    ? base.__relationships__
    : Array.isArray(origem.__relationships__)
    ? origem.__relationships__
    : [];

  if (!novosRelationships.length) {
    if (existentes.length && !base.__relationships__) {
      base.__relationships__ = existentes;
    }
    return base;
  }

  const byId = new Map();

  const adicionar = (rel) => {
    if (!rel || typeof rel !== 'object') return;
    const idKey = String(rel.Id ?? '');
    if (!byId.has(idKey)) {
      byId.set(idKey, {
        Id: rel.Id,
        childrens: Array.isArray(rel.childrens) ? [...rel.childrens] : [],
        records: Array.isArray(rel.records) ? [...rel.records] : [],
      });
    } else {
      const acc = byId.get(idKey);
      if (Array.isArray(rel.childrens)) acc.childrens.push(...rel.childrens);
      if (Array.isArray(rel.records)) acc.records.push(...rel.records);
    }
  };

  existentes.forEach(adicionar);
  novosRelationships.forEach(adicionar);

  base.__relationships__ = Array.from(byId.values()).filter(
    (rel) =>
      (Array.isArray(rel.records) && rel.records.length > 0) ||
      (Array.isArray(rel.childrens) && rel.childrens.length > 0)
  );

  return base;
}

/**
 * Caminho COMPLETO (normalizacao de tipos + relationships).
 * Usado quando USOU TEMPLATE (integracoes genericas).
 */
function limparRegistroPlano(registro) {
  const normalizado = normalizarValor(registro);

  if (
    !normalizado ||
    typeof normalizado !== 'object' ||
    Array.isArray(normalizado)
  ) {
    return normalizado;
  }

  const { base, relationships: novosRelationships } =
    montarRelationshipsPorHeaders(normalizado);

  return mesclarRelationships(base, normalizado, novosRelationships);
}

/**
 * Caminho SUPER LEVE: usado no ENVIO DIRETO.
 * - Nao normaliza tipos (mantem o que veio da planilha/constantes).
 * - So converte *_rel[ID] em __relationships__.
 */
function limparRegistroDireto(registro) {
  if (
    !registro ||
    typeof registro !== 'object' ||
    Array.isArray(registro)
  ) {
    return registro;
  }

  const { base, relationships: novosRelationships } =
    montarRelationshipsPorHeadersSemNormalizar(registro);

  return mesclarRelationships(base, registro, novosRelationships);
}

module.exports = {
  limparRegistroPlano,
  limparRegistroDireto,
  montarRelationshipsPorHeaders,
};

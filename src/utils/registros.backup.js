// src/utils/registros.js
const { normalizarDataHora } = require('./normalizadores');

/**
 * Converte valores de forma segura:
 * - '' | null | undefined => undefined
 * - 'true'/'false' (case-insensitive) => boolean
 * - números em string (com vírgula/.) => Number
 * - datas => usa normalizarDataHora (mantém se não for data válida)
 * - arrays/objetos => aplica recursivamente e remove undefined
 *
 * ⚠️ Usado APENAS para caminhos que PRECISAM de normalização (template/integração).
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

/**
 * Tenta descobrir o GUID do registro pai em diferentes convenções.
 */
function extrairParentGuid(obj) {
  if (!obj || typeof obj !== 'object') return null;
  return (
    obj.__recordguid__ ||
    obj.__record_guid__ ||
    obj.recordguid ||
    null
  );
}

/**
 * Helper para dividir o valor bruto dos campos *_rel[ID].
 * Não faz normalização de tipo — serve para modo DIRETO.
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
 * Versão COMPLETA (com normalização) — usada para TEMPLATE / INTEGRAÇÃO.
 * Cabeçalhos: nomeCampo_rel[10855] → relationships.
 */
function montarRelationshipsPorHeaders(registroNormalizado) {
  if (
    !registroNormalizado ||
    typeof registroNormalizado !== 'object' ||
    Array.isArray(registroNormalizado)
  ) {
    return { base: registroNormalizado, relationships: [] };
  }

  const relRegex = /^(.*)_rel\[(\d+)\]$/;
  const base = {};
  const camposRel = [];

  for (const [chave, valor] of Object.entries(registroNormalizado)) {
    const m = relRegex.exec(chave);
    if (m) {
      const nomeCampo = m[1]; // ex.: cli_emails
      const idStr = m[2];     // ex.: "10855"
      camposRel.push({ nomeCampo, idStr, valor });
    } else {
      base[chave] = valor;
    }
  }

  if (!camposRel.length) {
    return { base, relationships: [] };
  }

  const parentGuid =
    extrairParentGuid(base) || extrairParentGuid(registroNormalizado);

  const porId = {}; // idStr -> { Id, childrens, records: [] }

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
      const normVal = normalizarValor(val);
      if (normVal === undefined) return;
      if (!rel.records[idx]) rel.records[idx] = {};
      rel.records[idx][nomeCampo] = normVal;
    });
  }

  const relationships = Object.values(porId)
    .map((rel) => {
      const records = (rel.records || [])
        .filter((r) => r && Object.keys(r).length > 0)
        .map((r) => {
          const rec = { ...r };

          // filho NÃO deve carregar relationships aninhados
          if ('__relationships__' in rec) delete rec.__relationships__;
          if ('childrens' in rec) delete rec.childrens;

          if (
            parentGuid &&
            (rec.__record_parent_guid__ == null ||
              rec.__record_parent_guid__ === '')
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

  return { base, relationships };
}

/**
 * Versão LEVE (sem normalizar tipos) — usada para ENVIO DIRETO.
 * Mesma ideia de montarRelationshipsPorHeaders, só que sem normalizarValor.
 */
function montarRelationshipsPorHeadersSemNormalizar(registroOriginal) {
  if (
    !registroOriginal ||
    typeof registroOriginal !== 'object' ||
    Array.isArray(registroOriginal)
  ) {
    return { base: registroOriginal, relationships: [] };
  }

  const relRegex = /^(.*)_rel\[(\d+)\]$/;
  const base = {};
  const camposRel = [];

  for (const [chave, valor] of Object.entries(registroOriginal)) {
    const m = relRegex.exec(chave);
    if (m) {
      const nomeCampo = m[1];
      const idStr = m[2];
      camposRel.push({ nomeCampo, idStr, valor });
    } else {
      base[chave] = valor;
    }
  }

  if (!camposRel.length) {
    return { base, relationships: [] };
  }

  const parentGuid =
    extrairParentGuid(base) || extrairParentGuid(registroOriginal);

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
      if (!rel.records[idx]) rel.records[idx] = {};
      rel.records[idx][nomeCampo] = val;
    });
  }

  const relationships = Object.values(porId)
    .map((rel) => {
      const records = (rel.records || [])
        .filter((r) => r && Object.keys(r).length > 0)
        .map((r) => {
          const rec = { ...r };

          // limpa qualquer lixo de relationships interno
          if ('__relationships__' in rec) delete rec.__relationships__;
          if ('childrens' in rec) delete rec.childrens;

          if (
            parentGuid &&
            (rec.__record_parent_guid__ == null ||
              rec.__record_parent_guid__ === '')
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

  return { base, relationships };
}

/**
 * Caminho COMPLETO (normalização de tipos + relationships).
 * Usado quando USOU TEMPLATE (integrações genéricas).
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

  const existentes = Array.isArray(base.__relationships__)
    ? base.__relationships__
    : Array.isArray(normalizado.__relationships__)
    ? normalizado.__relationships__
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
 * Caminho SUPER LEVE: usado no ENVIO DIRETO.
 * - Não normaliza tipos (mantém o que veio da planilha/constantes).
 * - Só converte *_rel[ID] em __relationships__.
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

  const existentes = Array.isArray(base.__relationships__)
    ? base.__relationships__
    : Array.isArray(registro.__relationships__)
    ? registro.__relationships__
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

module.exports = {
  limparRegistroPlano,
  limparRegistroDireto,
  montarRelationshipsPorHeaders,
};

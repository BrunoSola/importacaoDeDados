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
 * ⚠️ Esta função está MANTIDA com o mesmo comportamento original.
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
 * Tenta descobrir o GUID do registro pai em diferentes convenções:
 * - __recordguid__
 * - __record_guid__
 * - recordguid   (caso venha direto da planilha)
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
 * Lê campos no formato "<campo>_rel[ID]" e monta estrutura de relationships.
 *
 * Regras:
 * - Cabeçalho/chave: nomeCampo_rel[10855]
 *   → nomeCampo = "cli_emails", Id = 10855
 * - Valor:
 *   - string com "|" => split por "|"
 *   - array => usado diretamente
 *   - outro tipo => vira [valor]
 * - Vários campos com mesma ID (ex.: tel_numero_rel[20001], tel_tipo_rel[20001])
 *   → combinados por índice em records[0], records[1], ...
 *
 * OBS:
 * - Recebe o OBJETO já normalizado por normalizarValor.
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

  // Nenhum campo *_rel[ID] → nada a fazer
  if (!camposRel.length) {
    return { base, relationships: [] };
  }

  const parentGuid =
    extrairParentGuid(base) || extrairParentGuid(registroNormalizado);

  const porId = {}; // idStr -> { Id, childrens, records: [] }

  for (const { nomeCampo, idStr, valor } of camposRel) {
    if (valor === undefined || valor === null || valor === '') continue;

    let valoresArray;

    if (Array.isArray(valor)) {
      valoresArray = valor;
    } else if (typeof valor === 'string') {
      valoresArray = valor
        .split('|')
        .map((s) => s.trim())
        .filter(Boolean);
    } else {
      valoresArray = [valor];
    }

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
 * Aplica normalização recursiva, remove chaves indefinidas
 * e converte campos *_rel[ID] em __relationships__.
 *
 * Compatibilidade:
 * - Se NÃO existir nenhum campo *_rel[ID]:
 *   → comportamento igual ao antigo (apenas normalizarValor).
 * - Se JÁ existir __relationships__ (montado pelo template):
 *   → mescla com os relationships vindos de *_rel[ID], agrupando por Id.
 */
function limparRegistroPlano(registro) {
  // 1) mantém a MESMA normalização que você já tinha
  const normalizado = normalizarValor(registro);

  if (
    !normalizado ||
    typeof normalizado !== 'object' ||
    Array.isArray(normalizado)
  ) {
    return normalizado;
  }

  // 2) Extrai relationships baseados em cabeçalho *_rel[ID]
  const { base, relationships: novosRelationships } =
    montarRelationshipsPorHeaders(normalizado);

  // 3) Relationships existentes (se vieram do template)
  const existentes = Array.isArray(base.__relationships__)
    ? base.__relationships__
    : Array.isArray(normalizado.__relationships__)
    ? normalizado.__relationships__
    : [];

  if (!novosRelationships.length) {
    // Sem *_rel[ID]: apenas garante que __relationships__ existente não se perca
    if (existentes.length && !base.__relationships__) {
      base.__relationships__ = existentes;
    }
    return base;
  }

  // 4) Mescla existentes + novos por Id
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
 * Versão específica para ENVIO DIRETO:
 * - NÃO faz normalização de tipos (mantém strings como vieram do XLSX/CSV).
 * - Apenas converte campos *_rel[ID] em __relationships__, removendo-os do root.
 */
function montarRelationshipsPorHeadersSemNormalizar(registro) {
  if (
    !registro ||
    typeof registro !== 'object' ||
    Array.isArray(registro)
  ) {
    return { base: registro, relationships: [] };
  }

  const relRegex = /^(.*)_rel\[(\d+)\]$/;
  const base = {};
  const camposRel = [];

  for (const [chave, valor] of Object.entries(registro)) {
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
    extrairParentGuid(base) || extrairParentGuid(registro);

  const porId = {}; // idStr -> { Id, childrens, records: [] }

  for (const { nomeCampo, idStr, valor } of camposRel) {
    if (valor === undefined || valor === null || valor === '') continue;

    let valoresArray;
    if (Array.isArray(valor)) {
      valoresArray = valor;
    } else if (typeof valor === 'string') {
      valoresArray = valor
        .split('|')
        .map((s) => s.trim())
        .filter(Boolean);
    } else {
      valoresArray = [valor];
    }

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
      // 👇 aqui NÃO chamamos normalizarValor, mantemos o tipo original
      rel.records[idx][nomeCampo] = val;
    });
  }

  const relationships = Object.values(porId)
    .map((rel) => {
      const records = (rel.records || [])
        .filter((r) => r && Object.keys(r).length > 0)
        .map((r) => {
          const rec = { ...r };
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
 * Limpeza específica para ENVIO DIRETO:
 * - Mantém todos os tipos como vieram do template/arquivo (sem normalizar).
 * - Só converte *_rel[ID] em __relationships__ e mescla com existentes.
 */
function limparRegistroDireto(registro) {
  if (
    !registro ||
    typeof registro !== 'object' ||
    Array.isArray(registro)
  ) {
    return registro;
  }

  const { base, relationships: novos } =
    montarRelationshipsPorHeadersSemNormalizar(registro);

  if (!novos.length) {
    return base;
  }

  const existentes = Array.isArray(base.__relationships__)
    ? base.__relationships__
    : [];

  if (!existentes.length) {
    base.__relationships__ = novos;
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
  novos.forEach(adicionar);

  base.__relationships__ = Array.from(byId.values());

  return base;
}


module.exports = {
  limparRegistroPlano,
  montarRelationshipsPorHeaders,
  limparRegistroDireto,
};

// src/transformers/templatePayload.js

// -------- preencherMarcadores: preenche {{coluna}} quando existir (rápido) --------
function preencherMarcadores(modelo, linha) {
  if (Array.isArray(modelo)) {
    const out = new Array(modelo.length);
    for (let i = 0; i < modelo.length; i++) out[i] = preencherMarcadores(modelo[i], linha);
    return out;
  }
  if (modelo && typeof modelo === 'object') {
    const out = {};
    for (const k in modelo) {
      if (Object.prototype.hasOwnProperty.call(modelo, k)) {
        out[k] = preencherMarcadores(modelo[k], linha);
      }
    }
    return out;
  }
  if (typeof modelo === 'string') {
    // atalho: sem placeholder → retorna direto
    if (modelo.indexOf('{{') === -1) return modelo;
    return modelo.replace(/\{\{([^}]+)\}\}/g, (_, name) => {
      const chave = name.trim();
      return linha[chave] ?? '';
    });
  }
  return modelo;
}

// -------- autoMapearPorChave: copia valores quando modelo[k] === "" e linha possui k --------
function autoMapearPorChave(obj, linha) {
  if (!obj || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) {
    const out = new Array(obj.length);
    for (let i = 0; i < obj.length; i++) out[i] = autoMapearPorChave(obj[i], linha);
    return out;
  }
  const out = {};
  for (const k in obj) {
    if (!Object.prototype.hasOwnProperty.call(obj, k)) continue;
    const v = obj[k];
    if (v === '' && Object.prototype.hasOwnProperty.call(linha, k)) {
      out[k] = linha[k];
    } else if (v && typeof v === 'object') {
      out[k] = autoMapearPorChave(v, linha);
    } else {
      out[k] = v;
    }
  }
  return out;
}

// -------- helpers de GUID do pai --------
function obterGuidPai(obj) {
  if (obj && typeof obj === 'object') {
    if (obj.__recordguid__) return obj.__recordguid__;
    if (obj.__record_guid__) return obj.__record_guid__; // fallback aceito
  }
  return null;
}

// Preenche __record_parent_guid__ nos filhos quando vier null/vazio
function anexarGuidPai(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  const guidPai = obterGuidPai(obj);
  const rels = obj.__relationships__;
  if (Array.isArray(rels) && guidPai) {
    for (let i = 0; i < rels.length; i++) {
      const r = rels[i];
      if (r && Array.isArray(r.records)) {
        for (let j = 0; j < r.records.length; j++) {
          const rec = r.records[j];
          if (rec && (rec.__record_parent_guid__ == null || rec.__record_parent_guid__ === '')) {
            rec.__record_parent_guid__ = guidPai;
          }
        }
      }
    }
  }

  // aplica recursivamente em estruturas internas (se existirem)
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) anexarGuidPai(obj[i]);
  } else {
    for (const k in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) {
        const v = obj[k];
        if (v && typeof v === 'object') anexarGuidPai(v);
      }
    }
  }
  return obj;
}

// -------- limpeza (opcional) --------
function limparVazios(valor) {
  if (Array.isArray(valor)) {
    const out = [];
    for (let i = 0; i < valor.length; i++) {
      const v = limparVazios(valor[i]);
      if (v === '' || v == null) continue;
      if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0) continue;
      out.push(v);
    }
    return out;
  }
  if (valor && typeof valor === 'object') {
    const out = {};
    for (const k in valor) {
      if (!Object.prototype.hasOwnProperty.call(valor, k)) continue;
      const v = limparVazios(valor[k]);
      const descartar =
        v === '' ||
        v == null ||
        (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0);
      if (!descartar) out[k] = v;
    }
    return out;
  }
  return valor;
}

/**
 * construirPayload(linhas, modelo, opcoes)
 * - linhas: array de objetos lidos do arquivo (CSV/XLSX/XML)
 * - modelo: objeto/array com valores literais e/ou placeholders {{coluna}}
 * - opcoes:
 *   - removeEmpty (boolean, default true): limpa chaves vazias
 *   - single (boolean, default false): retorna só o 1º registro
 *   - autoMapSameNames (boolean, default true): se modelo[k] === "" e linha[k] existe, copia
 *
 * Observações:
 * - GUIDs literais do modelo são preservados (não geramos GUID dinâmico).
 * - __record_parent_guid__ é preenchido com o GUID do pai quando vier null/vazio.
 */
function construirPayload(linhas, modelo, opcoes = {}) {
  const {
    removeEmpty = true,
    single = false,
    autoMapSameNames = true,
  } = opcoes;

  const out = new Array(linhas.length);
  for (let i = 0; i < linhas.length; i++) {
    const linha = linhas[i];

    // 1) preencher {{placeholders}}
    const comPlaceholders = preencherMarcadores(modelo, linha);

    // 2) auto-map "" -> linha[k]
    const mapeado = autoMapSameNames ? autoMapearPorChave(comPlaceholders, linha) : comPlaceholders;

    // 3) GUID pai nos filhos
    const comPai = anexarGuidPai(mapeado);

    // 4) limpeza opcional
    out[i] = removeEmpty ? limparVazios(comPai) : comPai;
  }

  return single ? (out[0] ?? null) : out;
}

// Exporta com nome em PT-BR e mantém compatibilidade com o nome antigo
module.exports = { construirPayload, buildPayload: construirPayload };

// src/handler.parts/template.js
//
// Objetivo:
// - Se existir template (header/body), construir payload(s) a partir das linhas do arquivo.
// - Em seguida aplicar CONSTANTES (sempre) e SANITIZAR (apenas quando usou template).
// - Devolver as linhas finais prontas para envio (direto/integracao) e o indicador usouTemplate.
//
// Fontes aceitas para template (prioridade):
//  1) Header: x-template-json-b64 (base64 de JSON)
//  2) Header: x-template-json     (JSON em texto)
//  3) Header: x-template-json-url (JSON URL-encoded)
//  4) Body:   event.template_b64  (base64 de JSON)
//  5) Body:   event.template      (objeto ou string JSON)
//  6) Body:   event.payloadTemplate (objeto ou string JSON)
//
// Opções (headers):
// - x-template-single: 'true' para produzir um único payload agregando todas as linhas
// - x-template-remove-empty: 'true' (default) para remover campos vazios
// - x-template-auto-map: 'true' (default) para mapear colunas com mesmo nome

const { extrairConstantes, aplicarConstantes } = require('../utils/constantes');
const { limparRegistroPlano } = require('../utils/registros');

function parseMaybeJson(str) {
  try { return JSON.parse(String(str)); } catch { return null; }
}

function getTemplateFromHeadersOrBody({ event, headers }) {
  const tplB64Hdr = headers['x-template-json-b64'];
  const tplJsonHdr = headers['x-template-json'];
  const tplJsonUrlHdr = headers['x-template-json-url'];

  const tplBodyB64 = event && event.template_b64;
  const tplBodyRaw = event && (event.template ?? event.payloadTemplate);

  let template = null;
  let fonte = null;

  if (tplB64Hdr) {
    const jsonStr = Buffer.from(String(tplB64Hdr), 'base64').toString('utf8');
    template = parseMaybeJson(jsonStr);
    fonte = 'hdr_b64';
  } else if (tplJsonHdr) {
    template = parseMaybeJson(String(tplJsonHdr));
    fonte = 'hdr_json';
  } else if (tplJsonUrlHdr) {
    template = parseMaybeJson(decodeURIComponent(String(tplJsonUrlHdr)));
    fonte = 'hdr_url';
  } else if (tplBodyB64) {
    const jsonStr = Buffer.from(String(tplBodyB64), 'base64').toString('utf8');
    template = parseMaybeJson(jsonStr);
    fonte = 'body_b64';
  } else if (typeof tplBodyRaw === 'string') {
    template = parseMaybeJson(tplBodyRaw);
    fonte = 'body_str';
  } else if (tplBodyRaw && typeof tplBodyRaw === 'object') {
    template = tplBodyRaw;
    fonte = 'body_obj';
  }

  if (Array.isArray(template)) {
    if (template.length === 0) throw new Error('Template array vazio');
    console.warn('[template] recebido como array — usando o primeiro item.');
    template = template[0];
  }

  return { template, fonte };
}

/** Aplica template, se houver; caso contrário, devolve as linhas originais */
function applyTemplateIfAny({ linhas, event, headers }) {
  const { template, fonte } = getTemplateFromHeadersOrBody({ event, headers });
  if (!template) {
    return { usouTemplate: false, linhas };
  }

  // Opções de construção do payload
  const single = String(headers['x-template-single'] || '').toLowerCase() === 'true';
  const removeEmpty = String(headers['x-template-remove-empty'] || 'true').toLowerCase() !== 'false';
  const autoMapSameNames = String(headers['x-template-auto-map'] || 'true').toLowerCase() !== 'false';

  try {
    const { construirPayload } = require('../transformers/templatePayload'); // carregado sob demanda
    const built = construirPayload(linhas, template, { removeEmpty, single, autoMapSameNames });
    const result = single ? (built ? [built] : []) : (built || []);
    console.log(`[template] aplicado: fonte=${fonte}, single=${single}, removeEmpty=${removeEmpty}, autoMap=${autoMapSameNames}, linhas=${result.length}`);
    return { usouTemplate: true, linhas: result };
  } catch (e) {
    const msg = e?.message || 'Erro ao processar template';
    const err = new Error(`Template inválido: ${msg}`);
    err.statusCode = 400;
    throw err;
  }
}

/** Aplica CONSTANTES sempre; se usou template, sanitiza (remove campos internos do template) */
function applyConstantesAndSanitize({ registros, headersLower, headersRaw, overrideConsts, usouTemplate }) {
  const consts = extrairConstantes(headersLower, headersRaw);

  const aplicados = (Array.isArray(registros) ? registros : [registros]).map((r) =>
    aplicarConstantes(r, consts, overrideConsts)
  );

  const finais = usouTemplate ? aplicados.map(limparRegistroPlano) : aplicados;

  return { consts, registrosProcessados: finais };
}

module.exports = { applyTemplateIfAny, applyConstantesAndSanitize };

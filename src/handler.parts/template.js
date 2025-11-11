// src/handler.parts/template.js
const { extrairConstantes, aplicarConstantes } = require('../utils/constantes');
const { limparRegistroPlano } = require('../utils/registros');

function resolveTemplateFromSources(event, headers) {
  // fontes: hdr b64/json/url + body b64/json
  const tplB64Hdr = headers['x-template-json-b64'];
  const tplJsonHdr = headers['x-template-json'];
  const tplJsonUrlHdr = headers['x-template-json-url'];
  const tplBodyB64 = event.template_b64;
  const tplBodyRaw = event.template ?? event.payloadTemplate;

  const hasTemplate = !!tplB64Hdr || !!tplJsonHdr || !!tplJsonUrlHdr || !!tplBodyB64 || tplBodyRaw != null;
  if (!hasTemplate) return null;

  let template;
  if (tplB64Hdr) {
    const jsonStr = Buffer.from(String(tplB64Hdr), 'base64').toString('utf8');
    template = JSON.parse(jsonStr);
  } else if (tplJsonHdr) {
    template = JSON.parse(String(tplJsonHdr));
  } else if (tplJsonUrlHdr) {
    template = JSON.parse(decodeURIComponent(String(tplJsonUrlHdr)));
  } else if (tplBodyB64) {
    const jsonStr = Buffer.from(String(tplBodyB64), 'base64').toString('utf8');
    template = JSON.parse(jsonStr);
  } else if (typeof tplBodyRaw === 'string') {
    template = JSON.parse(tplBodyRaw);
  } else if (tplBodyRaw && typeof tplBodyRaw === 'object') {
    template = tplBodyRaw;
  } else {
    throw new Error('Template não encontrado em header/body');
  }

  if (Array.isArray(template)) {
    if (template.length === 0) throw new Error('Template array vazio');
    console.warn('[template] recebido como array — usando o primeiro item.');
    template = template[0];
  }

  const single = String(headers['x-template-single'] || '').toLowerCase() === 'true';
  const removeEmpty = String(headers['x-template-remove-empty'] || 'true').toLowerCase() !== 'false';
  const autoMapSameNames = String(headers['x-template-auto-map'] || 'true').toLowerCase() !== 'false';

  return { template, options: { single, removeEmpty, autoMapSameNames } };
}

function applyTemplateIfAny({ linhas, event, headers }) {
  const spec = resolveTemplateFromSources(event, headers);
  if (!spec) return { usouTemplate: false, linhas };
  const { construirPayload } = require('../transformers/templatePayload');
  const built = construirPayload(linhas, spec.template, spec.options);
  const linhasParaEnviar = spec.options.single ? (built ? [built] : []) : built;
  return { usouTemplate: true, linhas: linhasParaEnviar, spec };
}

function applyConstantesAndSanitize({ registros, headersLower, headersRaw, overrideConsts, usouTemplate }) {
  const consts = extrairConstantes(headersLower, headersRaw);
  const base = registros.map((r) => aplicarConstantes(r, consts, overrideConsts));
  return {
    consts,
    registrosProcessados: usouTemplate ? base.map(limparRegistroPlano) : base,
  };
}

module.exports = {
  resolveTemplateFromSources,
  applyTemplateIfAny,
  applyConstantesAndSanitize,
};

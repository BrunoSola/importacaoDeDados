// src/importers/nfe.js
// Transformer NF-e -> payload Flowch (pai + filhos)
// Dependências: fast-xml-parser, ../utils/fileParser (detectKind)
// Ajuste FORM_IDs e GUIDs conforme seu ambiente quando necessário.

const { XMLParser } = require('fast-xml-parser');
const { detectKind } = require('../utils/fileParser');

const FIXED_PARENT_GUID = '82024372-8ca7-869a-ac13-e0a3ef95396f';

// GUIDs fixos por tabela filha (conforme sua especificação)
const FIXED_PRODUTO_FORM_GUID    = '4665-asdf82-asdf894627';
const FIXED_DUPLICATAS_FORM_GUID = '1234-asdf82-asdf789456';
const FIXED_PAGAMENTOS_FORM_GUID = '4321-asdf82-asdf456789';

// IDs reais dos formulários no Flowch (substitua quando criar/editar formulários)
const ITENS_PRODUTO_FORM_ID = 10741;
const DUPLICATAS_FORM_ID    = 10742;
const PAGAMENTOS_FORM_ID    = 10743;

function looksLikeNFe({ contentType, filename, buffer }) {
  const isXml = detectKind({ contentType, filename }) === 'xml';
  if (!isXml) return false;
  const head = buffer?.slice(0, 32 * 1024)?.toString('utf8') || '';
  return /<(?:NFe|nfeProc)\b/i.test(head);
}

/* ---------- utilitários de normalização ---------- */

const onlyDigits = s => (s ? String(s).replace(/\D+/g, '') : null);

const toDecimal = (s, scale = 2) => {
  if (s == null || s === '') return null;
  const n = Number(String(s).replace(',', '.'));
  if (!Number.isFinite(n)) return null;
  const factor = Math.pow(10, scale);
  return Math.round(n * factor) / factor;
};

const toIsoDatetime = s => {
  if (!s) return null;
  const cleaned = String(s).trim();
  const d = new Date(cleaned);
  if (!Number.isNaN(d.getTime())) return d.toISOString();
  return null;
};

const ensureArray = v => (v == null ? [] : Array.isArray(v) ? v : [v]);

/* ---------- parser XML (remove namespace prefixes) ---------- */

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  removeNSPrefix: true,
  parseTagValue: false,
  parseAttributeValue: false,
  trimValues: true,
});

/* ---------- mapeamento do registro pai ---------- */

function mapPaiFromXml(parsed) {
  const nfeRoot = parsed?.nfeProc?.NFe?.infNFe ?? parsed?.NFe?.infNFe ?? parsed?.infNFe ?? null;
  const prot = parsed?.nfeProc?.protNFe ?? parsed?.protNFe ?? parsed?.protNfe ?? null;

  const ide = nfeRoot?.ide ?? {};
  const emit = nfeRoot?.emit ?? {};
  const dest = nfeRoot?.dest ?? {};
  const total = nfeRoot?.total?.ICMSTot ?? {};

  const pai = {};
  pai.__recordguid__ = FIXED_PARENT_GUID; // GUID do pai (fornecido)
  pai.tipo = 'NFE';
  pai.modelo = ide?.mod ? String(ide.mod) : '55';
  pai.serie = ide?.serie ?? null;
  pai.numero = ide?.nNF ?? null;
  pai.data_emissao = toIsoDatetime(ide?.dhEmi ?? ide?.dEmi ?? null);
  pai.data_saida_entrada = toIsoDatetime(ide?.dhSaiEnt ?? ide?.dSaiEnt ?? null);
  pai.ambiente = prot?.infProt?.tpAmb ?? prot?.tpAmb ?? ide?.tpAmb ?? null;
  pai.municipio_ibge = ide?.cMunFG ?? null;
  pai.uf = emit?.enderEmit?.UF ?? (emit?.enderEmit?.uf ?? null);
  pai.emitente_doc = onlyDigits(emit?.CNPJ ?? emit?.CPF ?? null);
  pai.emitente_doc_type = pai.emitente_doc && pai.emitente_doc.length === 14 ? 'CNPJ' : (pai.emitente_doc && pai.emitente_doc.length === 11 ? 'CPF' : null);
  pai.emitente_razao = emit?.xNome ?? null;
  pai.emitente_ie = emit?.IE ?? null;

  pai.destinatario_tomador_doc = onlyDigits(dest?.CNPJ ?? dest?.CPF ?? null);
  pai.destinatario_tomador_razao = dest?.xNome ?? null;
  pai.destinatario_ie = dest?.IE ?? null;

  pai.valor_total_nota = toDecimal(total?.vNF ?? total?.vTot ?? null, 2);
  pai.valor_produtos_total = toDecimal(total?.vProd ?? null, 2);
  pai.valor_desconto = toDecimal(total?.vDesc ?? null, 2);
  pai.valor_frete = toDecimal(total?.vFrete ?? null, 2) ?? 0;
  pai.valor_seguro = toDecimal(total?.vSeg ?? null, 2) ?? 0;
  pai.valor_outros = toDecimal(total?.vOutro ?? null, 2) ?? 0;
  pai.valor_icms = toDecimal(total?.vICMS ?? null, 2);
  pai.valor_ipi = toDecimal(total?.vIPI ?? null, 2);
  pai.valor_pis = toDecimal(total?.vPIS ?? null, 2) ?? 0;
  pai.valor_cofins = toDecimal(total?.vCOFINS ?? null, 2) ?? 0;
  pai.valor_tot_trib = toDecimal(total?.vTotTrib ?? null, 2);

  pai.nfe_chave = prot?.infProt?.chNFe ?? (nfeRoot?.['@_Id'] ? String(nfeRoot['@_Id']).replace(/^NFe/i, '') : null);
  if (pai.nfe_chave && pai.nfe_chave.startsWith('NFe')) pai.nfe_chave = pai.nfe_chave.replace(/^NFe/i, '');
  pai.fin_nfe = ide?.finNFe ?? null;
  pai.id_dest = ide?.idDest ? Number(String(ide.idDest)) : null;
  pai.ind_final = ide?.indFinal ? Number(String(ide.indFinal)) : null;
  pai.ind_pres = ide?.indPres ? Number(String(ide.indPres)) : null;

  pai.cstat = prot?.infProt?.cStat ?? null;
  pai.xmotivo = prot?.infProt?.xMotivo ?? null;
  pai.protocolo = prot?.infProt?.nProt ?? null;
  pai.data_autorizacao = toIsoDatetime(prot?.infProt?.dhRecbto ?? prot?.dhRecbto ?? null);

  pai.versao_xml = parsed?.nfeProc?.['@_versao'] ?? parsed?.['@_versao'] ?? parsed?.NFe?.infNFe?.['@_versao'] ?? null;

  // Não enviamos hash do XML (você pediu para não usar)
  pai.fonte_xml_hash = null;

  pai.observacoes_internas = (nfeRoot?.infNFe?.infAdic?.infCpl ?? nfeRoot?.infAdic?.infCpl ?? null) || null;

  return pai;
}

/* ---------- mapear itens de produto (det) - filhos usam GUID fixo por tabela ---------- */

function mapItemDet(detNode, parentGuid) {
  const prod = detNode?.prod ?? {};
  const imp = detNode?.imposto ?? {};

  const item = {};
  item.__record_parent_guid__ = parentGuid;
  item.__recordguid__ = FIXED_PRODUTO_FORM_GUID; // GUID fixo para TODOS os filhos dessa tabela

  item.n_item = detNode?.['@_nItem'] ?? detNode?.nItem ?? null;
  item.cprod = prod?.cProd ?? null;
  item.cean = prod?.cEAN ?? null;
  item.xprod = prod?.xProd ?? null;
  item.ncm = prod?.NCM ?? null;
  item.cest = prod?.CEST ?? null;
  item.cfop = prod?.CFOP ?? null;
  item.ucom = prod?.uCom ?? null;
  item.qcom = toDecimal(prod?.qCom ?? null, 4) ?? 0;
  item.vuncom = toDecimal(prod?.vUnCom ?? null, 4) ?? 0;
  item.vprod = toDecimal(prod?.vProd ?? null, 2) ?? 0;
  item.vdesc = toDecimal(prod?.vDesc ?? null, 2) ?? 0;
  item.ceantrib = prod?.cEANTrib ?? null;
  item.utrib = prod?.uTrib ?? null;
  item.qtrib = toDecimal(prod?.qTrib ?? null, 4) ?? 0;
  item.vuntrib = toDecimal(prod?.vUnTrib ?? null, 4) ?? 0;
  item.ind_tot = prod?.indTot ?? null;

  const icms = imp?.ICMS ?? null;
  if (icms) {
    const icmsKey = Object.keys(icms).find(k => k && typeof icms[k] === 'object');
    const icmsObj = icmsKey ? icms[icmsKey] : icms;
    item.icms_origem = icmsObj?.orig ?? null;
    item.icms_cst_csosn = icmsObj?.CST ?? icmsObj?.CSOSN ?? null;
    item.icms_modalidade_bc = icmsObj?.modBC ?? null;
    item.icms_p_red_bc = icmsObj?.pRedBC ?? null;
    item.icms_vbc = toDecimal(icmsObj?.vBC ?? null, 2);
    item.icms_picms = toDecimal(icmsObj?.pICMS ?? null, 4);
    item.icms_vicms = toDecimal(icmsObj?.vICMS ?? null, 2);
    item.icms_vbcst = toDecimal(icmsObj?.vBCST ?? null, 2);
    item.icms_picmsst = toDecimal(icmsObj?.pICMSST ?? null, 4);
    item.icms_vicmsst = toDecimal(icmsObj?.vICMSST ?? null, 2);
  }

  const ipi = imp?.IPI ?? null;
  if (ipi) {
    const ipiObj = ipi?.IPITrib ?? ipi;
    item.ipi_cenq = ipi?.cEnq ?? null;
    item.ipi_cst = ipiObj?.CST ?? null;
    item.ipi_vbc = toDecimal(ipiObj?.vBC ?? null, 2);
    item.ipi_pipi = toDecimal(ipiObj?.pIPI ?? null, 4);
    item.ipi_vipi = toDecimal(ipiObj?.vIPI ?? null, 2);
  }

  const pis = imp?.PIS ?? null;
  if (pis) {
    const pisKey = Object.keys(pis).find(k => k && typeof pis[k] === 'object');
    const pisObj = pisKey ? pis[pisKey] : pis;
    item.pis_cst = pisObj?.CST ?? null;
    item.pis_vbc = toDecimal(pisObj?.vBC ?? null, 2);
    item.pis_ppis = toDecimal(pisObj?.pPIS ?? null, 4);
    item.pis_vpis = toDecimal(pisObj?.vPIS ?? null, 2);
  }

  const cof = imp?.COFINS ?? null;
  if (cof) {
    const cofKey = Object.keys(cof).find(k => k && typeof cof[k] === 'object');
    const cofObj = cofKey ? cof[cofKey] : cof;
    item.cofins_cst = cofObj?.CST ?? null;
    item.cofins_vbc = toDecimal(cofObj?.vBC ?? null, 2);
    item.cofins_pcofins = toDecimal(cofObj?.pCOFINS ?? null, 4);
    item.cofins_vcofins = toDecimal(cofObj?.vCOFINS ?? null, 2);
  }

  return item;
}

/* ---------- duplicatas (filho) ---------- */

function mapDuplicatas(nfeRoot, parentGuid) {
  const cobr = nfeRoot?.cobr ?? {};
  const dups = ensureArray(cobr?.dup ?? []);
  return dups.map(d => ({
    __record_parent_guid__: parentGuid,
    __recordguid__: FIXED_DUPLICATAS_FORM_GUID,
    n_dup: d?.nDup ?? null,
    d_venc: d?.dVenc ?? null,
    v_dup: toDecimal(d?.vDup ?? null, 2)
  }));
}

/* ---------- pagamentos (filho) ---------- */

function mapPagamentos(nfeRoot, parentGuid) {
  const pagArray = ensureArray(nfeRoot?.pag?.detPag ?? nfeRoot?.pag ?? []);
  return pagArray.map((p, idx) => ({
    __record_parent_guid__: parentGuid,
    __recordguid__: FIXED_PAGAMENTOS_FORM_GUID,
    idx: idx + 1,
    t_pag: p?.tPag ?? null,
    x_pag: p?.xPag ?? null,
    v_pag: toDecimal(p?.vPag ?? null, 2),
    troco: toDecimal(p?.vTroco ?? null, 2) || 0,
    cnpj_credenciadora: p?.CNPJ ?? null,
    tband: p?.tBand ?? null,
    caut: p?.cAut ?? null
  }));
}

/* ---------- função principal de transformação ---------- */

async function transformNfe({ xmlBuffer, filename = null, resolveMunicipio = async () => null, logger = console }) {
  const xmlStr = Buffer.isBuffer(xmlBuffer) ? xmlBuffer.toString('utf8') : String(xmlBuffer);
  const parsed = parser.parse(xmlStr);

  const nfeRoot = parsed?.nfeProc?.NFe?.infNFe ?? parsed?.NFe?.infNFe ?? parsed?.infNFe ?? null;
  if (!nfeRoot) throw new Error('unsupported_document_type_or_invalid_nfe');

  const pai = mapPaiFromXml(parsed);

  const ibge = pai.municipio_ibge ?? null;
  if (ibge) {
    try {
      const mid = await resolveMunicipio(String(ibge));
      if (!mid) {
        throw new Error(`municipio_not_found:${ibge}`);
      }
      pai.municipio_id = mid;
    } catch (err) {
      logger.error('resolveMunicipio failed', err?.message || err);
      throw err;
    }
  }

  const detNodes = ensureArray(nfeRoot?.det ?? []);
  const itensRecords = detNodes.map(det => mapItemDet(det, pai.__recordguid__));
  const dups = mapDuplicatas(nfeRoot, pai.__recordguid__);
  const pags = mapPagamentos(nfeRoot, pai.__recordguid__);

  const relationships = [];
  if (itensRecords.length) relationships.push({ Id: ITENS_PRODUTO_FORM_ID, childrens: [], records: itensRecords });
  if (dups.length) relationships.push({ Id: DUPLICATAS_FORM_ID, childrens: [], records: dups });
  if (pags.length) relationships.push({ Id: PAGAMENTOS_FORM_ID, childrens: [], records: pags });

  const payload = {
    __recordguid__: pai.__recordguid__,
    ...pai,
    __relationships__: relationships
  };

  return [payload];
}

module.exports = {
  FIXED_PARENT_GUID,
  FIXED_PRODUTO_FORM_GUID,
  FIXED_DUPLICATAS_FORM_GUID,
  FIXED_PAGAMENTOS_FORM_GUID,
  ITENS_PRODUTO_FORM_ID,
  DUPLICATAS_FORM_ID,
  PAGAMENTOS_FORM_ID,
  looksLikeNFe,
  transformNfe
};

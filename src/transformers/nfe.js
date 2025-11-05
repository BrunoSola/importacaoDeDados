// src/transformers/nfe.js
// NF-e -> Payload Flowch (pai + filhos) com GUIDs/Ids fixos e alto desempenho.
// Depende de: fast-xml-parser (^4).
// Compatibilidade: exporta looksLikeNFe, transformNfe e transformNfeRows (alias).

const { XMLParser } = require('fast-xml-parser');
const { detectKind } = require('../utils/fileParser');

// ===== GUIDs fixos =====
const RECORDGUID_PAI               = '82024372-8ca7-869a-ac13-e0a3ef95396f';
const RECORDGUID_PRODUTO           = '4665-asdf82-asdf894627';
const RECORDGUID_SERVICO           = '4665-asdf82-asdf894682';
const RECORDGUID_PAGAMENTO         = '4665-asdf82-asdf894683';
const RECORDGUID_PARCELA_DUPLICATAS= '4665-asdf82-asdf894684';

// ===== Ids fixos (relacionamentos) =====
const ID_PRODUTOS   = 10741;
const ID_SERVICOS   = 11021;
const ID_PAGAMENTOS = 11022;
const ID_DUPLICATAS = 11023;

// ===== Heurística para reconhecer NF-e =====
function looksLikeNFe({ contentType, filename, buffer }) {
  const isXml = detectKind({ contentType, filename }) === 'xml';
  if (!isXml) return false;
  const head = buffer?.slice(0, Math.min(buffer.length, 64 * 1024)).toString('utf8') || '';
  // nfeProc / NFe
  return /<(?:nfeProc|NFe)\b/i.test(head);
}

// ===== Utilitários de normalização super rápidos =====
const somenteDigitos = (s) => (s ? String(s).replace(/\D+/g, '') : null);

function toDecimal(s, scale = 2) {
  if (s == null || s === '') return null;
  const n = Number(String(s).replace(',', '.'));
  if (!Number.isFinite(n)) return null;
  const f = Math.pow(10, scale);
  return Math.round(n * f) / f;
}

function toIsoDatetime(s) {
  if (!s) return null;
  const d = new Date(String(s).trim());
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

const ensureArray = (v) => (v == null ? [] : Array.isArray(v) ? v : [v]);

// Parser sem parse automático de números/atributos para manter controle e performance
const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  removeNSPrefix: true,
  parseTagValue: false,
  parseAttributeValue: false,
  trimValues: true,
});

// ===== Mapear PAI =====
function mapearPai(parsed) {
  const nfeInf = parsed?.nfeProc?.NFe?.infNFe ?? parsed?.NFe?.infNFe ?? parsed?.infNFe ?? null;
  const prot   = parsed?.nfeProc?.protNFe ?? parsed?.protNFe ?? parsed?.protNfe ?? null;

  const ide  = nfeInf?.ide ?? {};
  const emit = nfeInf?.emit ?? {};
  const dest = nfeInf?.dest ?? {};
  const tot  = nfeInf?.total?.ICMSTot ?? {};

  const out = {};
  out.__recordguid__ = RECORDGUID_PAI;

  // Cabeçalho/identificação
  out.tipo   = 'NFE';
  out.modelo = ide?.mod ? String(ide.mod) : '55';
  out.serie  = ide?.serie ?? null;
  out.numero = ide?.nNF ?? null;

  out.data_emissao        = toIsoDatetime(ide?.dhEmi ?? ide?.dEmi ?? null);
  out.data_saida_entrada  = toIsoDatetime(ide?.dhSaiEnt ?? ide?.dSaiEnt ?? null);

  // Competência = AAAA-MM-01 (se houver data de emissão)
  if (out.data_emissao) {
    const d = new Date(out.data_emissao);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    out.competencia = `${y}-${m}-01`;
  } else {
    out.competencia = null;
  }

  out.ambiente = prot?.infProt?.tpAmb ?? prot?.tpAmb ?? ide?.tpAmb ?? null;

  // Município / UF
  out.municipio_ibge_raw = ide?.cMunFG ?? null;
  out.municipio_id       = null; // será preenchido externamente (resolveMunicipio)
  out.uf = emit?.enderEmit?.UF ?? emit?.enderEmit?.uf ?? null;

  // Observações / chave / flags
  out.observacoes_internas = (nfeInf?.infNFe?.infAdic?.infCpl ?? nfeInf?.infAdic?.infCpl ?? null) || null;

  // Chave: do protNFe ou do @Id do infNFe (removendo "NFe")
  out.nfe_chave =
    prot?.infProt?.chNFe ??
    (nfeInf?.['@_Id'] ? String(nfeInf['@_Id']).replace(/^NFe/i, '') : null);

  out.fin_nfe  = ide?.finNFe ?? null;
  out.id_dest  = ide?.idDest ? Number(String(ide.idDest)) : null;
  out.ind_final= ide?.indFinal ? Number(String(ide.indFinal)) : null;
  out.ind_pres = ide?.indPres ? Number(String(ide.indPres)) : null;

  // NFSe campos (não se aplicam em NF-e)
  out.nfse_codigo_verificacao = null;
  out.nfse_numero = null;
  out.prestador_inscricao_municipal = null;

  // Emitente / Destinatário
  out.emitente_doc = somenteDigitos(emit?.CNPJ ?? emit?.CPF ?? null);
  out.emitente_razao = emit?.xNome ?? null;
  out.emitente_ie    = emit?.IE ?? null;

  out.destinatario_tomador_doc   = somenteDigitos(dest?.CNPJ ?? dest?.CPF ?? null);
  out.destinatario_tomador_razao = dest?.xNome ?? null;
  out.destinatario_ie            = dest?.IE ?? null;

  // Totais
  out.valor_total_nota     = toDecimal(tot?.vNF, 2);
  out.valor_produtos_total = toDecimal(tot?.vProd, 2);
  out.valor_servicos_total = null; // não se aplica a NF-e
  out.valor_desconto       = toDecimal(tot?.vDesc, 2);
  out.valor_frete          = toDecimal(tot?.vFrete, 2) ?? 0;
  out.valor_seguro         = toDecimal(tot?.vSeg, 2) ?? 0;
  out.valor_outros         = toDecimal(tot?.vOutro, 2) ?? 0;
  out.valor_icms           = toDecimal(tot?.vICMS, 2);
  out.valor_ipi            = toDecimal(tot?.vIPI, 2);
  out.valor_pis            = toDecimal(tot?.vPIS, 2) ?? 0;
  out.valor_cofins         = toDecimal(tot?.vCOFINS, 2) ?? 0;
  out.valor_iss            = null; // não se aplica a NF-e
  out.valor_tot_trib       = toDecimal(tot?.vTotTrib, 2);

  // Autorização
  out.cstat           = prot?.infProt?.cStat ?? null;
  out.xmotivo         = prot?.infProt?.xMotivo ?? null;
  out.protocolo       = prot?.infProt?.nProt ?? null;
  out.data_autorizacao= toIsoDatetime(prot?.infProt?.dhRecbto ?? prot?.dhRecbto ?? null);

  // Versão e hash de origem
  out.versao_xml     = parsed?.nfeProc?.['@_versao'] ?? parsed?.['@_versao'] ?? parsed?.NFe?.infNFe?.['@_versao'] ?? null;
  out.fonte_xml_hash = null; // por desempenho/privacidade, não calculado aqui

  return out;
}

// ===== Mapear itens de produto (det) =====
function mapearProduto(detNode, parentGuid) {
  const prod = detNode?.prod ?? {};
  const imp  = detNode?.imposto ?? {};

  const item = {};
  item.__record_parent_guid__ = parentGuid;
  item.__recordguid__         = RECORDGUID_PRODUTO;

  item.n_item = detNode?.['@_nItem'] ?? detNode?.nItem ?? null;

  item.cprod = prod?.cProd ?? null;
  item.cean  = prod?.cEAN ?? null;
  item.xProd = prod?.xProd ?? null;
  item.ncm   = prod?.NCM ?? null;
  item.cest  = prod?.CEST ?? null;
  item.cfop  = prod?.CFOP ?? null;

  item.ucom  = prod?.uCom ?? null;
  item.qcom  = toDecimal(prod?.qCom, 4) ?? 0;
  item.vuncom= toDecimal(prod?.vUnCom, 4) ?? 0;
  item.vprod = toDecimal(prod?.vProd, 2) ?? 0;
  item.vdesc = toDecimal(prod?.vDesc, 2) ?? 0;

  item.ceantrib = prod?.cEANTrib ?? null;
  item.utrib    = prod?.uTrib ?? null;
  item.qtrib    = toDecimal(prod?.qTrib, 4) ?? 0;
  item.vuntrib  = toDecimal(prod?.vUnTrib, 4) ?? 0;
  item.ind_tot  = prod?.indTot ?? null;

  // Tributação
  const icms = imp?.ICMS ?? null;
  if (icms) {
    const icmsKey = Object.keys(icms).find(k => k && typeof icms[k] === 'object');
    const icmsObj = icmsKey ? icms[icmsKey] : icms;
    item.icms_origem     = icmsObj?.orig ?? null;
    item.icms_cst_csosn  = icmsObj?.CST ?? icmsObj?.CSOSN ?? null;
    item.icms_modalidade_bc = icmsObj?.modBC ?? null;
    item.icms_p_red_bc   = icmsObj?.pRedBC ?? null;
    item.icms_vbc        = toDecimal(icmsObj?.vBC, 2);
    item.icms_picms      = toDecimal(icmsObj?.pICMS, 4);
    item.icms_vicms      = toDecimal(icmsObj?.vICMS, 2);
    item.icms_vbcst      = toDecimal(icmsObj?.vBCST, 2);
    item.icms_picmsst    = toDecimal(icmsObj?.pICMSST, 4);
    item.icms_vicmsst    = toDecimal(icmsObj?.vICMSST, 2);
  }

  const ipi = imp?.IPI ?? null;
  if (ipi) {
    const ipiObj = ipi?.IPITrib ?? ipi;
    item.ipi_cst  = ipiObj?.CST ?? null;
    item.ipi_cenq = ipi?.cEnq ?? null;
    item.ipi_vbc  = toDecimal(ipiObj?.vBC, 2);
    item.ipi_pipi = toDecimal(ipiObj?.pIPI, 4);
    item.ipi_vipi = toDecimal(ipiObj?.vIPI, 2);
  }

  const pis = imp?.PIS ?? null;
  if (pis) {
    const k = Object.keys(pis).find(x => x && typeof pis[x] === 'object');
    const p = k ? pis[k] : pis;
    item.pis_cst = p?.CST ?? null;
    item.pis_vbc = toDecimal(p?.vBC, 2);
    item.pis_ppis= toDecimal(p?.pPIS, 4);
    item.pis_vpis= toDecimal(p?.vPIS, 2);
  }

  const cof = imp?.COFINS ?? null;
  if (cof) {
    const k = Object.keys(cof).find(x => x && typeof cof[x] === 'object');
    const c = k ? cof[k] : cof;
    item.cofins_cst    = c?.CST ?? null;
    item.cofins_vbc    = toDecimal(c?.vBC, 2);
    item.cofins_pcofins= toDecimal(c?.pCOFINS, 4);
    item.cofins_vcofins= toDecimal(c?.vCOFINS, 2);
  }  

  return item;
}

// ===== Mapear serviços (NFSe) — para NF-e ficará vazio, mas mantemos a estrutura =====
function mapearServico(_node, parentGuid) {
  return {
    __record_parent_guid__: parentGuid,
    __recordguid__: RECORDGUID_SERVICO,
    seq: null,
    C99_FINANCEIRO_NOTA_FISCAL_Id: null,
    codigo_servico: null,
    codigo_tributacao_municipio: null,
    cnae: null,
    discriminacao: null,
    quantidade: null,
    valor_unit: null,
    valor_total: null,
    aliquota_iss: null,
    valor_iss: null,
    iss_retido: null,
    valor_inss: null,
    valor_ir: null,
    valor_pis: null,
    valor_cofins: null,
    valor_csll: null,
  };
}

// ===== Duplicatas (cobr/dup) =====
function mapearDuplicatas(nfeInf, parentGuid) {
  const dups = ensureArray(nfeInf?.cobr?.dup ?? []);
  return dups.map(d => ({
    __record_parent_guid__: parentGuid,
    __recordguid__: RECORDGUID_PARCELA_DUPLICATAS,
    n_dup: d?.nDup ?? null,
    d_venc: d?.dVenc ?? null,
    v_dup: toDecimal(d?.vDup, 2),
  }));
}

// ===== Pagamentos (pag/detPag) =====
function mapearPagamentos(nfeInf, parentGuid) {
  const arr = ensureArray(nfeInf?.pag?.detPag ?? nfeInf?.pag ?? []);
  return arr.map((p, idx) => ({
    __record_parent_guid__: parentGuid,
    __recordguid__: RECORDGUID_PAGAMENTO,
    idx: idx + 1,
    t_pag: p?.tPag ?? null,
    x_pag: p?.xPag ?? null,
    v_pag: toDecimal(p?.vPag, 2),
    troco: toDecimal(p?.vTroco, 2) || 0,
    cnpj_credenciadora: p?.CNPJ ?? null,
    tband: p?.tBand ?? null,
    caut: p?.cAut ?? null,
  }));
}

// ===== Principal =====
async function transformNfe({ xmlBuffer, filename = null, resolveMunicipio = async () => null, logger = console }) {
  const xmlStr = Buffer.isBuffer(xmlBuffer) ? xmlBuffer.toString('utf8') : String(xmlBuffer);
  const parsed = parser.parse(xmlStr);

  const nfeInf = parsed?.nfeProc?.NFe?.infNFe ?? parsed?.NFe?.infNFe ?? parsed?.infNFe ?? null;
  if (!nfeInf) throw new Error('unsupported_document_type_or_invalid_nfe');

  // Pai
  const pai = mapearPai(parsed);

  // Municipio_id por função externa (mantém performance e responsabilidades separadas)
  const ibge = pai.municipio_ibge_raw ?? null;
  if (ibge) {
    try {
      const mid = await resolveMunicipio(String(ibge));
      pai.municipio_id = mid ?? null;
    } catch (err) {
      logger.error('resolveMunicipio error:', err?.message || err);
      throw err;
    }
  }

  // Filhos
  const det = ensureArray(nfeInf?.det ?? []);
  const produtos   = det.map(d => mapearProduto(d, pai.__recordguid__));
  // Serviços: não existem em NF-e, mantemos vazio (estrutura pronta para NFSe se precisar)
  console.log(JSON.stringify('GUID DO PAI:', pai.__recordguid__));
  const servicos   = [];
  const duplicatas = mapearDuplicatas(nfeInf, pai.__recordguid__);
  const pagamentos = mapearPagamentos(nfeInf, pai.__recordguid__);

  const relationships = [];
  if (produtos.length)   relationships.push({ Id: ID_PRODUTOS,   childrens: [], records: produtos });
  if (servicos.length)   relationships.push({ Id: ID_SERVICOS,   childrens: [], records: servicos });
  if (pagamentos.length) relationships.push({ Id: ID_PAGAMENTOS, childrens: [], records: pagamentos });
  if (duplicatas.length) relationships.push({ Id: ID_DUPLICATAS, childrens: [], records: duplicatas });

  const payload = {
    __recordguid__: pai.__recordguid__,
    __relationships__: relationships,
    // Campos do pai depois dos metadados (mantém legível)
    ...pai,
  };

  // Retorno como array (compatível com o pipeline de envio em lotes)
  return [payload];
}

// Compat + exports
module.exports = {
  // GUIDs/Ids expostos (úteis para testes)
  RECORDGUID_PAI,
  RECORDGUID_PRODUTO,
  RECORDGUID_SERVICO,
  RECORDGUID_PAGAMENTO,
  RECORDGUID_PARCELA_DUPLICATAS,
  ID_PRODUTOS,
  ID_SERVICOS,
  ID_PAGAMENTOS,
  ID_DUPLICATAS,

  // API pública
  looksLikeNFe,
  transformNfe,
  transformNfeRows: transformNfe,
};

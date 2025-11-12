// src/transformers/nfse.js — NFSe (SPED) -> objeto plano com campos comuns ausentes preenchidos como null

/**
 * Este transformer aceita:
 *   - string XML da NFSe, ou
 *   - objeto já parseado (ex.: vindo do fast-xml-parser)
 *
 * Exporta:
 *   - looksLikeNFSe(input): boolean
 *   - transformNfse(input): objeto (1 NFSe)
 *   - transformNfseRows(input): array de 1 (compat)
 *
 * Desempenho:
 *   - parsing leve (fast-xml-parser) somente quando a entrada é string
 *   - apenas leituras diretas (sem caminhadas profundas custosas)
 */

const { XMLParser } = require('fast-xml-parser');

// --------------- helpers leves ---------------

function isString(v) {
  return typeof v === 'string' || v instanceof String;
}

function safeNum(v) {
  if (v == null || v === '') return null;
  const s = String(v).trim();
  const n = Number(s.replace(/\./g, '').replace(',', '.'));
  return Number.isFinite(n) ? n : Number(s);
}

function tsToIsoLocal(ts) {
  if (ts == null || ts === '') return null;
  // Mantém exatamente "YYYY-MM-DDTHH:mm:ss-03:00" → "YYYY-MM-DD HH:mm:ss"
  const m = String(ts).match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
  if (!m) return String(ts);
  return `${m[1]} ${m[2]}:${m[3]}:${m[4]}`;
}

/** Pega caminho com pontos sem custo alto, com tolerância a faltas. */
function pick(obj, path) {
  if (!obj || !path) return undefined;
  const parts = String(path).split('.').filter(Boolean);
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

/** Detecta estrutura da NFSe (SPED) */
function looksLikeNFSe(input) {
  const root = parseIfXml(input, { shallow: true });
  return !!(root && (root.NFSe || root?.['nfsProc'] || root?.['Rps'] || root?.['NFSeProc']));
}

/** Faz parse só quando for string; caso contrário retorna o próprio objeto */
function parseIfXml(input, { shallow = false } = {}) {
  if (!isString(input)) return input || null;
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@',
    allowBooleanAttributes: true,
    trimValues: true,
    parseTagValue: false,
    parseAttributeValue: false,
  });
  try {
    const doc = parser.parse(input || '');
    if (shallow) return doc || null;
    return doc || null;
  } catch {
    return null;
  }
}

// --------------- mapeador principal ---------------

/**
 * Normaliza a NFSe com base no layout SPED (como no seu exemplo).
 * Também preenche diversos campos COMUNS em NFSe (porém ausentes no XML) com null,
 * para padronizar o esquema e facilitar persistência/integração depois.
 */
function transformNfse(input) {
  const doc = parseIfXml(input) || {};
  // Estrutura típica do exemplo:
  // { NFSe: { versao, infNFSe: { ..., DPS: { infDPS: {...} } }, Signature: ... } }
  const NFSe = doc.NFSe || doc.nfse || null;
  if (!NFSe) {
    // Pode existir "NFSeProc" em alguns municípios; aqui mantemos simples
    throw new Error('Documento não parece ser uma NFSe compatível (nó <NFSe> ausente).');
  }

  const versao = NFSe.versao ?? NFSe['@versao'] ?? null;
  const inf = NFSe.infNFSe || NFSe.infNfse || {};
  const dps = (inf.DPS && (inf.DPS.infDPS || inf.DPS.infDps)) || {};

  // --- campos diretos de <infNFSe> ---
  const out = {
    // Identificadores/ambiente
    nfse_id: inf['@Id'] || inf.Id || null,
    nfse_versao: versao ?? (inf.versao || null),

    // Localidades
    nfse_x_loc_emi: inf.xLocEmi ?? null,
    nfse_x_loc_prestacao: inf.xLocPrestacao ?? null,
    nfse_numero: inf.nNFSe ?? null,
    nfse_cod_municipio_incid: inf.cLocIncid ?? null,
    nfse_municipio_incid: inf.xLocIncid ?? null,

    // Tributação nacional / NBS (SPED)
    nfse_x_trib_nac: inf.xTribNac ?? null,
    nfse_x_nbs: inf.xNBS ?? null,

    // Aplicativo/ambiente/emissão
    nfse_ver_aplic: inf.verAplic ?? null,
    nfse_ambiente_ger: inf.ambGer ?? null,
    nfse_tp_emis: inf.tpEmis ?? null,
    nfse_proc_emi: inf.procEmi ?? null,
    nfse_cstat: inf.cStat ?? null,
    nfse_dh_proc: tsToIsoLocal(inf.dhProc) ?? null,
    nfse_ndfse: inf.nDFSe ?? null,
  };

  // --- emitente <emit> ---
  const emit = inf.emit || {};
  const enderNac = emit.enderNac || {};
  out.emit_cnpj = emit.CNPJ ?? null;
  out.emit_razao = emit.xNome ?? null;
  out.emit_fone = emit.fone ?? null;
  out.emit_email = emit.email ?? null;
  out.emit_end_xlgr = enderNac.xLgr ?? null;
  out.emit_end_nro = enderNac.nro ?? null;
  out.emit_end_xbairro = enderNac.xBairro ?? null;
  out.emit_end_cmun = enderNac.cMun ?? null;
  out.emit_end_uf = enderNac.UF ?? null;
  out.emit_end_cep = enderNac.CEP ?? null;

  // --- totais <valores> de infNFSe ---
  const valores = inf.valores || {};
  out.total_retencoes = safeNum(valores.vTotalRet);
  out.valor_liquido = safeNum(valores.vLiq);

  // --- bloco DPS (<DPS><infDPS>) ---
  out.dps_tp_amb = dps.tpAmb ?? null;
  out.dps_dh_emi = tsToIsoLocal(dps.dhEmi) ?? null;
  out.dps_ver_aplic = dps.verAplic ?? null;
  out.dps_serie = dps.serie ?? null;
  out.dps_numero = dps.nDPS ?? null;
  out.dps_competencia = dps.dCompet ?? null; // já vem AAAA-MM-DD
  out.dps_tp_emit = dps.tpEmit ?? null;
  out.dps_c_loc_emi = dps.cLocEmi ?? null;

  // prestador dentro do DPS (redundante com <emit>, mas mantemos)
  const prest = dps.prest || {};
  out.prest_cnpj = prest.CNPJ ?? null;
  out.prest_fone = prest.fone ?? null;
  out.prest_email = prest.email ?? null;
  out.prest_reg_op_simp_nac = pick(prest, 'regTrib.opSimpNac') ?? null;
  out.prest_reg_esp_trib = pick(prest, 'regTrib.regEspTrib') ?? null;

  // tomador
  const toma = dps.toma || {};
  out.toma_cnpj = toma.CNPJ ?? null;
  out.toma_im = toma.IM ?? null;
  out.toma_razao = toma.xNome ?? null;

  // endereço tomador (tem um nível <end><endNac/> … em alguns esquemas)
  const end = toma.end || {};
  const endNacTom = end.endNac || {};
  out.toma_end_cmun = endNacTom.cMun ?? null;
  out.toma_end_cep = endNacTom.CEP ?? null;
  out.toma_end_xlgr = end.xLgr ?? null;
  out.toma_end_nro = end.nro ?? null;
  out.toma_end_xbairro = end.xBairro ?? null;
  out.toma_fone = toma.fone ?? null;
  out.toma_email = toma.email ?? null;

  // serviços
  const serv = dps.serv || {};
  out.loc_prest_codigo = pick(serv, 'locPrest.cLocPrestacao') ?? null;
  out.serv_trib_nac = pick(serv, 'cServ.cTribNac') ?? null;
  out.serv_desc = pick(serv, 'cServ.xDescServ') ?? null;
  out.serv_nbs = pick(serv, 'cServ.cNBS') ?? null;
  out.serv_info_compl = pick(serv, 'infoCompl.xInfComp') ?? null;

  // valores/tributação no DPS
  const dpsValores = dps.valores || {};
  out.v_servicos = safeNum(pick(dpsValores, 'vServPrest.vServ'));
  const trib = dpsValores.trib || {};
  const tribMun = trib.tribMun || {};
  out.issqn_tributado = pick(tribMun, 'tribISSQN') ?? null; // 1 = sim (no exemplo)
  out.issqn_tipo_retencao = pick(tribMun, 'tpRetISSQN') ?? null;
  out.ind_total_trib = pick(trib, 'totTrib.indTotTrib') ?? null;

  // ---------------------- CAMPOS COMUNS (AUSENTES NO XML) ----------------------
  // Vários municípios usam estruturas baseadas na ABRASF/Nacional. Mantemos campos padrões comuns como null.
  // Isso simplifica (1) persistência, (2) joins e (3) futuras integrações para quando esses campos existirem.
  Object.assign(out, {
    // Identificação do RPS / NFSe
    rps_numero: null,
    rps_serie: null,
    rps_tipo: null, // 1=RPS, 2=RPS-M, etc

    // Natureza / regime / opções
    natureza_operacao: null, // 1..6 (ex.: 1=Tributação no município)
    regime_tributacao: null, // 1=MEI/Simples, etc (pode variar)
    optante_simples: null,   // 1=Sim, 2=Não
    incentivador_cultural: null, // 1=Sim, 2=Não

    // Serviço / atividade
    codigo_servico: null,           // (itemListaServico / código municipal)
    item_lista_servico: null,       // (ABRASF)
    codigo_tributacao_municipio: null,
    codigo_cnae: null,
    discriminacao: null,            // descrição livre (quando não vier em xDescServ)
    exigibilidade_iss: null,        // 1=Exigível, 2=Não incidência, etc
    municipio_incidencia: null,     // código do município da incidência

    // Valores detalhados
    valor_servicos: out.v_servicos ?? null,
    valor_deducoes: null,
    valor_pis: null,
    valor_cofins: null,
    valor_inss: null,
    valor_ir: null,
    valor_csll: null,
    outras_retencoes: null,
    desconto_incondicionado: null,
    desconto_condicionado: null,
    aliquota_iss: null,        // em %
    iss_retido: null,          // 1=Sim, 2=Não
    responsavel_retencao: null // 1=Tomador, 2=Prestador (varia por layout)
  });

  // Pequena dedução: se veio nfse_x_trib_nac e não veio discriminacao, aproveitamos texto
  if (!out.discriminacao && out.serv_desc) {
    out.discriminacao = out.serv_desc;
  }

  return out;
}

function transformNfseRows(input) {
  return [transformNfse(input)];
}

module.exports = {
  looksLikeNFSe,
  transformNfse,
  transformNfseRows,
};

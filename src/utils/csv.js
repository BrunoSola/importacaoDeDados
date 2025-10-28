// src/utils/csv.js
// CSV robusto:
// - Detecta delimitador com heurística e tem FALLBACK baseado no cabeçalho real
// - Considera candidatos: ; , \t |
// - Trata BOM e normaliza cabeçalhos (remove BOM, tabs, NBSP, trim)
// - RFC4180-like: aspas duplas e escape ""

function stripBOM(text = '') {
  if (!text) return '';
  if (text.charCodeAt(0) === 0xFEFF) return text.slice(1);
  if (text.startsWith('\uFEFF')) return text.slice(1);
  return text;
}

function sanitizeHeaderName(h) {
  return String(h || '')
    .replace(/\ufeff/g, '')     // BOM interno
    .replace(/\u00a0/g, ' ')    // NBSP -> espaço normal
    .replace(/\t/g, '')         // remove tabs perdidos
    .replace(/\s+/g, ' ')       // colapsa espaços
    .trim();
}

/** Split linha CSV considerando aspas e escape "" */
function splitCsvLine(line, delimiter) {
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { cur += '"'; i++; }
      else { inQuotes = !inQuotes; }
    } else if (ch === delimiter && !inQuotes) {
      out.push(cur); cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

/** Heurística primária de delimitador */
function detectDelimiter(text) {
  const candidates = [';', ',', '\t', '|'];
  const lines = text.split(/\r?\n/).slice(0, 5).filter(l => l !== '');
  if (lines.length === 0) return ';';

  let best = { delim: ';', score: -Infinity, avg: 0, variance: Infinity };

  for (const d of candidates) {
    const counts = lines.map(l => splitCsvLine(l, d).length);
    const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
    const variance = counts.reduce((a, b) => a + Math.pow(b - avg, 2), 0) / counts.length;

    // Prioriza mais colunas e consistência
    const score = Math.log(1 + avg) - variance;

    const better =
      score > best.score ||
      (score === best.score && (avg > best.avg)) ||
      (score === best.score && avg === best.avg && d === ';' && best.delim !== ';');

    if (better) best = { delim: d, score, avg, variance };
  }

  return best.delim;
}

/** Fallback: se o cabeçalho não abriu, força um delimitador que abre */
function fallbackDelimiterByHeader(headerLine, chosen) {
  const tryDelims = [';', ',', '\t', '|'];

  // se o escolhido já abre (>1 coluna), mantém
  const colsChosen = splitCsvLine(headerLine, chosen);
  if (colsChosen.length > 1) return chosen;

  // tenta cada candidato que realmente aparece no header
  for (const d of tryDelims) {
    if (!headerLine.includes(d)) continue;
    const cols = splitCsvLine(headerLine, d);
    if (cols.length > 1) return d;
  }

  return chosen; // sem alternativa melhor
}

function parseCsvToObjects(text, opts = {}) {
  const { maxRows = 10000, maxCols = 200, forceDelimiter } = opts;
  if (!text || !text.length) return [];

  const clean = stripBOM(text);

  // separa linhas; remove apenas cauda vazia
  const rawLines = clean.split(/\r?\n/);
  let end = rawLines.length;
  while (end > 0 && rawLines[end - 1].trim() === '') end--;
  const lines = rawLines.slice(0, end);
  if (!lines.length) return [];

  // cabeçalho bruto
  const headerLineRaw = lines[0];

  // escolhe delimitador (com fallback baseado no header)
  let delimiter = forceDelimiter || detectDelimiter(clean);
  delimiter = fallbackDelimiterByHeader(headerLineRaw, delimiter);

  // cabeçalho final
  const headerRaw = splitCsvLine(headerLineRaw, delimiter);
  const header = headerRaw.map(sanitizeHeaderName);

  if (!header.length) throw new Error('Cabeçalho CSV ausente.');
  if (header.some(h => !h)) throw new Error('Cabeçalho CSV inválido: há coluna sem nome.');
  if (new Set(header).size !== header.length) throw new Error('Cabeçalho CSV inválido: colunas duplicadas.');
  if (header.length > maxCols) throw new Error(`CSV com colunas demais (${header.length} > ${maxCols}).`);

  const out = [];
  const dataLines = Math.min(lines.length - 1, maxRows);

  for (let i = 1; i <= dataLines; i++) {
    const line = lines[i] ?? '';
    const cols = splitCsvLine(line, delimiter);

    // ignora linha totalmente vazia
    if (cols.every(v => String(v ?? '').trim() === '')) continue;

    const obj = {};
    for (let c = 0; c < header.length; c++) {
      const raw = cols[c] ?? '';
      const val = typeof raw === 'string' ? raw.trim() : raw;
      obj[header[c]] = val;
    }
    out.push(obj);
  }

  // ⛑️ Segurança extra:
  // Se por algum motivo sobrou linha com "chave única" contendo vários ';' e valor idem,
  // "explode" dinamicamente para colunas.
  if (out.length && Object.keys(out[0]).length === 1) {
    const onlyKey = Object.keys(out[0])[0] || '';
    const looksPacked = onlyKey.includes(';');
    if (looksPacked) {
      // refaz mapeando com o mesmo delimitador decidido
      const fixed = [];
      for (let i = 1; i <= dataLines; i++) {
        const line = lines[i] ?? '';
        const vals = splitCsvLine(line, delimiter);
        const obj = {};
        for (let c = 0; c < header.length; c++) {
          obj[header[c]] = (vals[c] ?? '').trim();
        }
        // descarta linhas 100% vazias
        if (!Object.values(obj).every(v => String(v ?? '').trim() === '')) {
          fixed.push(obj);
        }
      }
      return fixed;
    }
  }

  return out;
}

module.exports = { parseCsvToObjects, detectDelimiter, splitCsvLine, sanitizeHeaderName };

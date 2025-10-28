// utils/fixPackedCsvRecord.js
function normalizeDate(val) {
    if (val == null) return val;
    const s = String(val).trim();
    let m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/); if (m) return `${m[3]}-${m[2]}-${m[1]}`;
    m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);       if (m) return s; // já ISO
    return s;
  }
  
  function sanitizeHeaderName(h) {
    return String(h || '')
      .replace(/\ufeff/g, '')     // remove BOM
      .replace(/\t/g, '')         // remove tabs
      .replace(/\s+/g, ' ')       // normaliza espaços
      .trim();
  }
  
  function maybeExplodePackedRecord(rec) {
    // Procura por uma chave com vários ';' e um valor também com ';'
    for (const [k, v] of Object.entries(rec)) {
      if (typeof k === 'string' && k.includes(';') && typeof v === 'string' && v.includes(';')) {
        const headers = k.split(';').map(sanitizeHeaderName).filter(Boolean);
        const values  = v.split(';').map(s => String(s).trim());
  
        const out = { ...rec };
        delete out[k];
  
        for (let i = 0; i < headers.length; i++) {
          let val = values[i] ?? '';
  
          // normaliza números com vírgula decimal (ex: 10,2 → 10.2)
          if (/^\d+,\d+$/.test(val)) val = val.replace(',', '.');
  
          // normaliza datas dd/MM/yyyy → ISO
          val = normalizeDate(val);
  
          out[headers[i]] = val;
        }
  
        return out;
      }
    }
    return rec;
  }
  
  module.exports = { maybeExplodePackedRecord };
  
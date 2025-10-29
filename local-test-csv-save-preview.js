// local-test-csv-save-preview.js
// Salva preview.json e imprime os registros parseados do sample.csv
const fs = require('fs');
const path = require('path');
const { prepararArquivo } = require('./src/services/arquivoService');

async function run() {
  const filePath = path.join(__dirname, 'sample_2000.csv');
  if (!fs.existsSync(filePath)) {
    console.error('Coloque sample.csv na raiz e rode novamente.');
    process.exit(1);
  }

  const buf = fs.readFileSync(filePath);
  const base64 = buf.toString('base64');

  const event = {
    headers: { 'Content-Type': 'application/json', 'x-preview': 'true' },
    body: JSON.stringify({
      base64,
      filename: 'sample.csv',
      contentType: 'text/csv'
    }),
  };

  try {
    const preparo = await prepararArquivo({
      event,
      headers: event.headers,
      gerarPreview: true,
      limitePreview: 10,
      limitarLinhas: null,
      formatarPreview: (regs) => regs
    });

    const out = {
      filename: preparo.arquivo.filename,
      contentType: preparo.arquivo.contentType,
      linhasLidas: preparo.linhas.length,
      preview: preparo.preview,
      primeirasLinhas: preparo.linhas.slice(0, 10)
    };

    fs.writeFileSync(path.join(__dirname, 'preview.json'), JSON.stringify(out, null, 2), 'utf8');
    console.log('preview.json salvo — linhasLidas:', preparo.linhas.length);
    console.log('Primeiras linhas (obj):');
    console.dir(preparo.linhas, { depth: 3, maxArrayLength: 10 });
  } catch (err) {
    console.error('Erro prepararArquivo:', err && err.message ? err.message : err);
    process.exit(2);
  }
}

run();

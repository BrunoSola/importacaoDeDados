// local-test-csv.js
// Teste local para importação CSV (modo dry-run)
// Coloque sample.csv na raiz do projeto antes de rodar.

const fs = require('fs');
const path = require('path');
const { handler } = require('./src/handler');

async function run() {
  const filePath = path.join(__dirname, 'sample.csv');
  if (!fs.existsSync(filePath)) {
    console.error('Coloque sample.csv na raiz do projeto e rode novamente.');
    process.exit(1);
  }

  const buf = fs.readFileSync(filePath);
  const base64 = buf.toString('base64');

  const event = {
    headers: {
      'Content-Type': 'application/json',
      'x-filename': 'sample.csv',
      'x-endpoint-url': 'https://flowch.fake/api/endpoint',
      'Authorization': 'dummy-token',
      'x-dry-run': 'true',
      'x-preview': 'true',
      // 'x-batch-size': '10',
      // 'x-log-progress': 'true'
    },
    body: JSON.stringify({
      base64,
      filename: 'sample.csv',
      contentType: 'text/csv'
    }),
    isBase64Encoded: false
  };

  const context = { getRemainingTimeInMillis: () => 60000 };

  try {
    const resp = await handler(event, context);
    console.log('Resposta do handler:');
    console.log(JSON.stringify(resp, null, 2));
  } catch (err) {
    console.error('Erro ao executar handler:', err);
  }
}

run();

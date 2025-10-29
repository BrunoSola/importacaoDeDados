//local-test.js

const { handler } = require('./src/handler');

(async () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<NFe xmlns="http://www.portalfiscal.inf.br/nfe">
  <infNFe versao="4.00" Id="112429102025">
    <det nItem="1"><prod><xProd>Produto Exemplo</xProd></prod></det>
    <det nItem="1"><prod><xProd>Produto Exemplo2</xProd></prod></det>
  </infNFe>
</NFe>`;
  const b64 = Buffer.from(xml, 'utf8').toString('base64');

  const event = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'cc7c8f7b148d178d1d81df4fa32b511e',
      'x-endpoint-url': 'https://int01.flowch.com/integrator/8523d68b-b0c6-4850-a1db-e6345b55a305/notaFiscal',
      'x-dry-run': 'false',
      'x-preview': 'true',
      'x-xml-record-path': 'NFe.infNFe.det',
      'x-child-form-id': 10741,
      'x-fixed-guid': '82024372-8ca7-869a-ac13-e0a3ef95396f',
      'x-xml-map': JSON.stringify({ nfe_chave: 'NFe.infNFe.@Id', xProd: 'prod.xProd' }),
    },
    isBase64Encoded: false,
    body: JSON.stringify({ base64: b64 })
  };
  const context = { getRemainingTimeInMillis: () => 25_000 };
  const resp = await handler(event, context);
  console.log(resp.statusCode, resp.body);
})();
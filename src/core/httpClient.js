const https = require('https');
const http = require('http');

const AGENTS = {
  'https': new https.Agent({ keepAlive: true, maxSockets: 64, maxFreeSockets: 16, scheduling: 'lifo' }),
  'http': new https.Agent({ keepAlive: true, maxSockets: 64, maxFreeSockets: 16, scheduling: 'lifo'})
}

function httpJson(urlStr, method, headers, payload, timeoutMs) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const isHttps = u.protocol === 'https:';
    const agent = AGENTS[u.protocol] || undefined;

    const hdrs = Object.assign(
      {'Accept': 'application/json', 'Content-Type': 'application/json', 'Connection': 'keep-alive'}, 
      headers);
    
    // Content-Length preciso (se houver payload)
    let payloadStr = '';
    if (payload) {
      payloadStr = typeof payload === 'string' ? payload : JSON.stringify(payload);
      hdrs['Content-Length'] = Buffer.byteLength(payloadStr).toString();
    }
    
    const options = {
      protocol: u.protocol,
      hostname: u.hostname,
      port: u.port || (isHttps ? 443 : 80),
      agent,
      path: u.pathname + (u.search || ''),
      method,
      headers: hdrs,
      timeout: timeoutMs,
      agent
    };

    const req = (isHttps ? https : http).request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: data }));
    });

    req.on('timeout', () => req.destroy(Object.assign(new Error(`Request timeout after ${timeoutMs}ms`), { code: 'ETIMEDOUT' })));
    req.on('error', reject);
    if (payload) req.write(payloadStr);
    req.end();
  });
}

module.exports = { httpJson };

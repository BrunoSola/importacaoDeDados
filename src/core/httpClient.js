// src/core/httpClient.js
//
// Objetivo:
// - Enviar requisições HTTP/HTTPS de forma performática e previsível na Lambda.
// - Suportar keep-alive (menos handshakes/TLS), timeout robusto e retries inteligentes.
// - Respeitar Retry-After quando houver (rate limit), com backoff exponencial + jitter.
// - Descompactar respostas gzip/deflate automaticamente (quando o servidor envia).
// - Nunca logar tokens em claro (Authorization mascarado).
//
// Compatibilidade:
// - Assinatura preservada: httpJson(urlStr, method, headers, payload, timeoutMs)
// - Retorno preservado: { statusCode, headers, body } (body como string)
// - Quem já faz JSON.parse(resp.body) continua funcionando.

const http = require('http');
const https = require('https');
const { URL } = require('url');
const zlib = require('zlib');

// =========================
// Configuração (ajustável via ENV, sem alterar código)
// =========================
//
// HTTP_KEEP_ALIVE: true/false (padrão true)
// HTTP_MAX_SOCKETS: limite de conexões simultâneas (padrão 64)
// HTTP_MAX_FREE_SOCKETS: conexões ociosas mantidas no pool (padrão 16)
// HTTP_MAX_RETRIES: tentativas adicionais além da 1ª (padrão 2 -> total de 3)
// HTTP_ACCEPT_ENCODING: "gzip,deflate" (padrão) ou vazio para desabilitar
const KEEP_ALIVE = String(process.env.HTTP_KEEP_ALIVE || 'true').toLowerCase() !== 'false';
const MAX_SOCKETS = Math.max(1, Number(process.env.HTTP_MAX_SOCKETS || 64));
const MAX_FREE_SOCKETS = Math.max(0, Number(process.env.HTTP_MAX_FREE_SOCKETS || 16));
const DEFAULT_MAX_RETRIES = Math.max(0, Number(process.env.HTTP_MAX_RETRIES || 2));
const ACCEPT_ENCODING = String(process.env.HTTP_ACCEPT_ENCODING ?? 'gzip,deflate');

const AGENTS = {
  'https:': new https.Agent({ keepAlive: KEEP_ALIVE, maxSockets: MAX_SOCKETS, maxFreeSockets: MAX_FREE_SOCKETS, scheduling: 'lifo' }),
  'http:':  new http.Agent ({ keepAlive: KEEP_ALIVE, maxSockets: MAX_SOCKETS, maxFreeSockets: MAX_FREE_SOCKETS, scheduling: 'lifo' }),
};

// =========================
// Utilitários de apoio (log seguro, Retry-After, backoff, descompressão)
// =========================
function maskTokenTail(value) {
  if (!value) return '';
  const asStr = String(value);
  return asStr.length <= 6 ? '****' : '****' + asStr.slice(-4);
}

function maskAuthorizationHeader(headers) {
  if (!headers) return headers;
  const out = { ...headers };
  const authKey = Object.keys(out).find(k => k.toLowerCase() === 'authorization');
  if (authKey && typeof out[authKey] === 'string') {
    out[authKey] = out[authKey].replace(/(integration\s+)?(.+)/i, (_, prefix, token) => (prefix || '') + maskTokenTail(token));
  }
  return out;
}

/** Converte Retry-After (segundos ou data RFC) em milissegundos */
function parseRetryAfterMs(responseHeaders) {
  const ra = responseHeaders['retry-after'] || responseHeaders['Retry-After'];
  if (!ra) return null;
  const secs = Number(ra);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const dateMs = Date.parse(ra);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return null;
}

/** Backoff exponencial com jitter para evitar “thundering herd” */
function computeBackoffWithJitterMs(attemptIndex, baseMs = 250, capMs = 4000) {
  const expo = Math.min(capMs, baseMs * Math.pow(2, attemptIndex - 1));
  const jitter = Math.floor(Math.random() * Math.min(500, expo));
  return expo + jitter;
}

/** Descompacta corpo se necessário (gzip/deflate) */
function decompressIfNeeded(responseHeaders, bodyBuffer) {
  const enc = String(responseHeaders['content-encoding'] || '').toLowerCase();
  if (enc.includes('gzip')) return zlib.gunzipSync(bodyBuffer);
  if (enc.includes('deflate')) return zlib.inflateSync(bodyBuffer);
  return bodyBuffer;
}

/** Decide se o status HTTP é elegível a retry (timeout, rate-limit, 5xx) */
function isRetryableStatus(statusCode) {
  return statusCode === 408 || statusCode === 429 || (statusCode >= 500 && statusCode < 600);
}

/** Erros de rede transitórios que valem retry */
function isTransientNetworkError(err) {
  return ['ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN', 'ECONNREFUSED', 'ENOTFOUND'].includes(err.code);
}

// =========================
// Função principal
// =========================
function httpJson(urlStr, method, headers, payload, timeoutMs) {
  const maxRetries = DEFAULT_MAX_RETRIES;

  // Hoist (melhora performance no retry): parse da URL, headers e payload só uma vez
  const parsedUrl = new URL(urlStr);
  const isHttps = parsedUrl.protocol === 'https:';
  const agent = AGENTS[parsedUrl.protocol];

  const effectiveHeaders = Object.assign(
    {
      'Accept': 'application/json',
      'Connection': 'keep-alive',
      ...(ACCEPT_ENCODING ? { 'Accept-Encoding': ACCEPT_ENCODING } : {})
    },
    headers || {}
  );

  let payloadString = '';
  if (payload !== undefined && payload !== null) {
    payloadString = (typeof payload === 'string' || Buffer.isBuffer(payload))
      ? String(payload)
      : JSON.stringify(payload);
    if (!('Content-Type' in effectiveHeaders) && !('content-type' in effectiveHeaders)) {
      effectiveHeaders['Content-Type'] = 'application/json';
    }
    effectiveHeaders['Content-Length'] = Buffer.byteLength(payloadString).toString();
  } else {
    delete effectiveHeaders['Content-Type'];
    delete effectiveHeaders['Content-Length'];
  }

  /** Dispara uma única tentativa */
  function doSingleAttempt(attemptNumber) {
    return new Promise((resolve, reject) => {
      const requestOptions = {
        protocol: parsedUrl.protocol,
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || (isHttps ? 443 : 80),
        path: parsedUrl.pathname + (parsedUrl.search || ''),
        method,
        headers: effectiveHeaders,
        agent,
      };

      const req = (isHttps ? https : http).request(requestOptions, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try {
            const raw = Buffer.concat(chunks);
            const dec = decompressIfNeeded(res.headers || {}, raw);
            const bodyStr = dec.toString('utf8');
            resolve({ statusCode: res.statusCode, headers: res.headers || {}, body: bodyStr });
          } catch (parseErr) {
            reject(parseErr);
          }
        });
      });

      // Pequena otimização de latência (Nagle off)
      req.setNoDelay(true);

      // Timeout por tentativa (gera ETIMEDOUT e será tratado como retryável)
      if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
        req.setTimeout(timeoutMs, () => {
          const timeoutError = Object.assign(new Error(`Request timeout after ${timeoutMs}ms`), { code: 'ETIMEDOUT' });
          req.destroy(timeoutError);
        });
      }

      req.on('error', reject);
      if (payloadString) req.write(payloadString);
      req.end();
    });
  }

  /** Loop com retries: respeita Retry-After e aplica backoff+Jitter */
  async function runWithRetries() {
    let lastError = null;

    for (let attemptNumber = 1; attemptNumber <= (1 + maxRetries); attemptNumber++) {
      try {
        const response = await doSingleAttempt(attemptNumber);

        if (response && isRetryableStatus(response.statusCode)) {
          if (attemptNumber <= maxRetries) {
            const retryAfterMs = parseRetryAfterMs(response.headers || {});
            const delayMs = (retryAfterMs != null) ? retryAfterMs : computeBackoffWithJitterMs(attemptNumber);
            console.warn(
              `[httpJson] retry status=${response.statusCode} attempt=${attemptNumber}/${1 + maxRetries} delayMs=${delayMs} url=${urlStr} headers=${JSON.stringify(maskAuthorizationHeader(headers || {}))}`
            );
            await new Promise(r => setTimeout(r, delayMs));
            continue;
          }
        }

        // Sucesso ou sem retry possível → retorna
        return response;

      } catch (err) {
        lastError = err;
        if (attemptNumber <= maxRetries && isTransientNetworkError(err)) {
          const delayMs = computeBackoffWithJitterMs(attemptNumber);
          console.warn(
            `[httpJson] error attempt=${attemptNumber}/${1 + maxRetries} delayMs=${delayMs} url=${urlStr} err=${err.code || err.message}`
          );
          await new Promise(r => setTimeout(r, delayMs));
          continue;
        }
        // Erro não-retryável ou sem tentativas restantes → propaga
        throw err;
      }
    }

    // Exaustão (não deveria chegar aqui)
    throw lastError || new Error('HTTP error (exhausted retries)');
  }

  return runWithRetries();
}

module.exports = { httpJson };

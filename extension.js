const { Readable } = require('node:stream');
const { createBrotliCompress, brotliDecompress, constants } = require('zlib');
const { HttpCache } = databases.cache;
/**
 * Setup the caching middleware
 */
exports.start = function (options = {}) {
	const servers = options.server.http(exports.getCacheHandler(options));
};

/**
 * This is the handler that is used to cache the response. It is defined and exported so other middleware can directly
 * use it and set a cacheKey or bypass the cache
 */
exports.getCacheHandler = function (options) {
	return async (request, nextHandler) => {
		if (request.method === 'POST' && request.url === '/invalidate' && request.user?.role.permission.super_user) {
			// invalidate the cache
			let last;
			for await (let entry of HttpCache.search([], { onlyIfCached: true, noCacheStore: true })) {
				last = HttpCache.delete(entry.id);
			}
			await last;
			return { status: 200, headers: {}, body: 'Cache invalidated' };
		}
		// check if the request is cacheable
		if (request.method === 'GET') {
			// assign the nextHandler so it can be used within the cache resolver
			request.cacheNextHandler = nextHandler;
			// use our cache table, using the cacheKey if provided, otherwise use the URL/path
			let response = await HttpCacheWithSWR.get(request.cacheKey ?? request.url, request);
			// if it is a cache miss, we let the handler actually directly write to the node response object
			// and stream the results to the client, so we don't need to return anything here
			if (!request._nodeResponse.writableEnded) {
				// but if we have a cache hit, we can return the cached response
				let ifNoneMatch = request.headers.get('If-None-Match');
				let headers = response.headers.toJSON();
				const etag = headers.etag;
				let status = response.status ?? 200;
				let body;
				let age = Math.round((Date.now() - response.getUpdatedTime()) / 1000);
				headers = { ...headers, 'X-HarperDB-Cache': 'HIT', Age: age };
				delete headers['x-harperdb-cache'];
				delete headers['content-length'];
				if (ifNoneMatch && ifNoneMatch === etag) {
					status = 304;
				} else {
					body = response.content;
					if (headers['content-encoding'] === 'br' && !request.headers.get('Accept-Encoding').includes('br')) {
						// if the client doesn't support brotli, we need to decompress the response
						body = await new Promise((resolve) => brotliDecompress(body, (err, result) => {
							if (err) reject(err);
							else resolve(result);
						}));
						delete headers['content-encoding'];
					}
					headers['Content-Length'] = body.length;
				}
				// for now, everything is being handled by the next.js server that writes to the node response object,
				// so can just assume that and not try to branch (and have to worry about testing the other branch)
				//if (request._nodeResponse.wroteHeaders) {
				request._nodeResponse.writeHead(status, headers);
				request._nodeResponse.end(body);
				/*} else {
					return {
						status,
						headers,
						body,
					};
				}*/
			}
		} else {
			// else we just let the handler write to the node response object
			return nextHandler(request);
		}
	};
};

/**
 * Source the Next.js cache from request resolution using the passed in Next.js request handler,
 * and intercepting the response to cache it.
 */
HttpCache.sourcedFrom({
	async get(path, context) {
		const request = context.requestContext;
		if (request.maxAgeSeconds) context.expiresAt = request.maxAgeSeconds * 1000 + Date.now();
		let expiresSWRAt;
		if (request.staleWhileRevalidateSeconds) {
			// this is the time at which the response can be served stale while revalidating, after the main expiresAt time
			expiresSWRAt = request.staleWhileRevalidateSeconds * 1000 + (context.expiresAt ?? Date.now());
		}
		return new Promise((resolve, reject) => {
			const nodeResponse = request._nodeResponse;
			if (!nodeResponse) return;
			// intercept the main methods to get and cache the response if the node response is directly used
			const writeHead = nodeResponse.writeHead;
			let encoder;
			nodeResponse.writeHead = (status, messageOrHeaders, headers) => {
				nodeResponse.setHeader('X-HarperDB-Cache', 'MISS');
				let headersObject =  headers ?? messageOrHeaders;
				getEncoder(headers?.['content-encoding']); // ensure the encoder is created, and Content-Encoding is set as
				// needed
				if (Array.isArray(messageOrHeaders?.[0])) {
					messageOrHeaders = messageOrHeaders.reduce((acc, [key, value]) => {
						acc[key] = value;
						return acc;
					}, {});
				}
				writeHead.call(nodeResponse, status, messageOrHeaders, headers);
			};
			function getEncoder(alreadyEncoded) {
				if (encoder) return encoder;
				let encoding = request.headers.get('Accept-Encoding');
				let accept = request.headers.get('Accept') ?? '';
				if (encoding.includes('br') && !alreadyEncoded) {
					nodeResponse.setHeader('Content-Encoding', 'br');
					nodeResponse.removeHeader('Content-Length');
					encoder = createBrotliCompress({
						params: {
							[constants.BROTLI_PARAM_MODE]:
								accept.includes('json') || accept.includes('text')
									? constants.BROTLI_MODE_TEXT
									: constants.BROTLI_MODE_GENERIC,
							[constants.BROTLI_PARAM_QUALITY]: 2, // go fast
						},
					})
					encoder.on('data', writeOut);
					encoder.on('end', endOut);
				} else {
					encoder = { // default direct encoder
						write: writeOut,
						end: endOut,
					};
				}
				return encoder;
			}
			const blocks = []; // collect the blocks of response data to cache
			const writeResponse = nodeResponse.write;
			const endResponse = nodeResponse.end;
			function writeOut(block) {
				if (typeof block === 'string') block = Buffer.from(block);
				blocks.push(block);
				writeResponse.call(nodeResponse, block)
			}
			function endOut(block) {
				if (block) {
					if (typeof block === 'string') block = Buffer.from(block);
					blocks.push(block);
				}
				endResponse.call(nodeResponse, block);
				const headers = Object.assign({}, nodeResponse.getHeaders());
				delete headers['x-harperdb-cache'];
				delete headers.connection;
				let etag = headers.etag;
				if (!etag) headers.etag = Date.now().toString(32);
				// cache the response, with the headers and content
				resolve({
					id: path,
					expiresSWRAt,
					headers,
					content: blocks.length > 1 ? Buffer.concat(blocks) : blocks[0],
				});
			}
			nodeResponse.write = (block) => {
				getEncoder().write(block);
			};
			nodeResponse.end = (block) => {
				// if the downstream handler is directly writing to the node response object, we need to capture and cache the
				// response
				if (nodeResponse.statusCode !== 200) {
					context.noCacheStore = true;
				}
				if (block instanceof ReadableStream) {
					const piped = Readable.fromWeb(block).pipe(encoder);
					piped.on('finish', () => {
						resolve({
							id: path,
							headers: nodeResponse.getHeaders(),
							//content: blocks.length > 1 ? Buffer.concat(blocks) : blocks[0],
						});
					});
					return;
				}
				getEncoder().end(block);
			};
			if (!request.cacheNextHandler) {
				return resolve();
			}
			let response = request.cacheNextHandler(request);
			if (response?.then) {
				response.then(forResponse);
			} else forResponse(response);
			function forResponse(response) {
				if (!response) return;
				if (response.status !== 200) context.noCacheStore = true;
				let headersObject = {};
				for (let [key, value] of response.headers) {
					headersObject[key] = value;
				}
				let cacheControl = response.headers.get('cache-control');
				exports.parseHeaderValue(cacheControl).forEach((part) => {
					if (part.name === 'no-store') context.noCacheStore = true;
					if (part.name === 'no-cache') context.noCache = true;
					if (part.name === 'max-age') context.expiresAt = part.value * 1000 + Date.now();
				});
				let etag = response.headers.get('ETag') || response.headers.get('Last-Modified');
				if (!etag) headersObject.ETag = Date.now().toString(32);
				// TODO: handle streaming responses
				let content = response.body;
				resolve({
					id: path,
					expiresSWRAt,
					headers: headersObject,
					content,
				});
			}
		});
	},
	name: 'http cache resolver',
});
class HttpCacheWithSWR extends HttpCache {
	// use directly URL paths for ids
	static parsePath(path) {
		return decodeURIComponent(path);
	}
	allowStaleWhileRevalidate(entry, id) {
		return entry.value?.expiresSWRAt > Date.now();
	}
}
/**
 * This parser is used to parse header values.
 *
 * It is used within this file for parsing the `Cache-Control` header.
 *
 * @param value
 */
exports.parseHeaderValue = function (value) {
	return value
		.trim()
		.split(',')
		.map((part) => {
			let parsed;
			const components = part.trim().split(';');
			let component;
			while ((component = components.pop())) {
				if (component.includes('=')) {
					let [name, value] = component.trim().split('=');
					name = name.trim();
					if (value) value = value.trim();
					parsed = {
						name: name.toLowerCase(),
						value,
						next: parsed,
					};
				} else {
					parsed = {
						name: component.toLowerCase(),
						next: parsed,
					};
				}
			}
			return parsed;
		});
};

const { HttpCache } = databases.cache;
/**
 * Setup the caching middleware
 */
export function start(options = {}) {
	const servers = options.server.http(getCacheHandler(options));
}

/**
 * This is the handler that is used to cache the response. It is defined and exported so other middleware can directly
 * use it and set a cacheKey or bypass the cache
 */
export function getCacheHandler(options) {
	return async (request, nextHandler) => {
		if (request.method === 'POST' && request.url === '/invalidate' && request) {
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
			let response = await HttpCache.get(request.cacheKey ?? request.url, request);
			// if it is a cache miss, we let the handler actually directly write to the node response object
			// and stream the results to the client, so we don't need to return anything here
			if (!request._nodeResponse.writableEnded) {
				// but if we have a cache hit, we can return the cached response
				return {
					status: 200,
					headers: { ...response.headers.toJSON(), 'X-HarperDB-Cache': 'HIT' },
					body: response.content,
				};
			}
		} else {
			// else we just let the handler write to the node response object
			return nextHandler(request);
		}
	}
}

/**
 * Source the Next.js cache from request resolution using the passed in Next.js request handler,
 * and intercepting the response to cache it.
 */
HttpCache.sourcedFrom({
	async get(path, context) {
		const request = context.requestContext;
		return new Promise((resolve, reject) => {
			const nodeResponse = request._nodeResponse;
			if (!nodeResponse) return;
			let cacheable;
			// intercept the main methods to get and cache the response if the node response is directly used
			const writeHead = nodeResponse.writeHead;
			nodeResponse.writeHead = (status, message, headers) => {
				nodeResponse.setHeader('X-HarperDB-Cache', 'MISS');
				if (status === 200) cacheable = true;
				writeHead.call(nodeResponse, status, message, headers);
			};
			const blocks = []; // collect the blocks of response data to cache
			const write = nodeResponse.write;
			nodeResponse.write = (block) => {
				if (typeof block === 'string') block = Buffer.from(block);
				blocks.push(block);
				write.call(nodeResponse, block);
			};
			const end = nodeResponse.end;
			nodeResponse.end = (block) => {
				// if the downstream handler is directly writing to the node response object, we need to capture and cache the
				// response
				if (block) {
					if (typeof block === 'string') block = Buffer.from(block);
					blocks.push(block);
				}
				end.call(nodeResponse, block);
				if (!cacheable) context.noCacheStore = true;
				// cache the response, with the headers and content
				resolve({
					id: path,
					headers: nodeResponse._headers,
					content: blocks.length > 1 ? Buffer.concat(blocks) : blocks[0],
				});
			};

			let response = request.cacheNextHandler(request);
			if (response?.then) {
				response.then(forResponse);
			} else forResponse(response);
			function forResponse(response) {
				if (!response) return;
				if (!cacheable) context.noCacheStore = true;
				let headersObject = {};
				for (let [key, value] of response.headers) {
					headersObject[key] = value;
				}
				let cacheControl = response.headers.get('cache-control');
				parseHeaderValue(cacheControl).forEach((part) => {
					if (part.name === 'no-store') context.noCacheStore = true;
					if (part.name === 'no-cache') context.noCache = true;
					if (part.name === 'max-age') context.expiresAt = part.value * 1000 + Date.now();
				});
				// TODO: handle streaming responses
				let content = response.body;
				resolve({
					id: path,
					headers: headersObject,
					content,
				});

			}
		});
	},
	name: 'http cache resolver',
});
/**
 * This parser is used to parse header values.
 *
 * It is used within this file for parsing the `Cache-Control` and `X-Replicate-To` headers.
 *
 * @param value
 */
export function parseHeaderValue(value) {
	return value.trim().split(',').map((part) => {
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
}

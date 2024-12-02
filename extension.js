const { HttpCache } = databases.cache;
/**
 * Setup the caching middleware
 */
export function start(options = {}) {
	const servers = options.server.http(async (request, nextHandler) => {
		if (config.cache && request.method === 'POST' && request.url === '/invalidate') {
			// invalidate the cache
			let last;
			for await (let entry of HttpCache.search([], { onlyIfCached: true, noCacheStore: true })) {
				last = HttpCache.delete(entry.id);
			}
			await last;
			return { status: 200, headers: {}, body: 'Cache invalidated' };
		}
		// check if the request is cacheable
		if (request.method === 'GET' && config.cache) {
			request.nextHandler = nextHandler;
			// use our cache table
			let response = await HttpCache.get(request.url, request);
			// if have cache miss, we let the handler actually directly write to the node response object
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
			return handler(request._nodeResponse);
		}
	});
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
			// intercept the main methods to get and cache the response
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
				// now we have the full response, cache it
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

			request.handler(nodeResponse);
		});
	},
	name: 'Http cache resolver',
});

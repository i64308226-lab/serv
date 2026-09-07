// @ts-ignore
import { connect } from 'cloudflare:sockets';

// Конфигурация: ваш UUID уже прописан ниже
let userID = '8addefb9-c255-439c-954d-3b01f4034d59';
let proxyIP = 'cdn-all.cloudflare.net';

if (!isValidUUID(userID)) {
	throw new Error('uuid is not valid');
}

export default {
	async fetch(request, env, ctx) {
		try {
			userID = env.UUID || userID;
			proxyIP = env.PROXYIP || proxyIP;
			const upgradeHeader = request.headers.get('Upgrade');
			if (!upgradeHeader || upgradeHeader !== 'websocket') {
				const url = new URL(request.url);
				switch (url.pathname) {
					case `/${userID}`: {
						const vlessConfig = makeVLESSConfig(userID, request.headers.get('Host'));
						return new Response(`${vlessConfig}`, {
							status: 200,
							headers: {
								"Content-Type": "text/html; charset=utf-8",
							},
						});
					}
					default:
						return new Response(JSON.stringify(request.cf, null, 4), { status: 200, headers: { "Content-Type": "application/json" } });
				}
			} else {
				return await vlessOverWSHandler(request);
			}
		} catch (err) {
			return new Response(err.toString(), { status: 500 });
		}
	},
};

async function vlessOverWSHandler(request) {
	const webSocketPair = new WebSocketPair();
	const [client, webSocket] = Object.values(webSocketPair);

	webSocket.accept();

	let address = '';
	let portWithRandomLog = '';
	const log = (info, event) => {
		console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
	};
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

	let remoteSocketWapper = {
		value: null,
	};
	let isDns = false;

	readableWebSocketStream.pipeTo(new WritableStream({
		async write(chunk, controller) {
			if (isDns) {
				return;
			}
			if (remoteSocketWapper.value) {
				const writer = remoteSocketWapper.value.writable.getWriter();
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}

			const vlessBuffer = chunk.buffer;
			if (vlessBuffer.byteLength < 24) {
				controller.error('invalid vless buffer length');
				return;
			}

			const version = new Uint8Array(vlessBuffer.slice(0, 1));
			const id = new Uint8Array(vlessBuffer.slice(1, 17));
			
			if (!stringify(id) === userID) {
				controller.error('invalid user ID');
				return;
			}

			const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
			const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 19 + optLength))[0];

			if (command === 1) {
			} else if (command === 2) {
			} else {
				controller.error(`command ${command} is not supported`);
				return;
			}
			const portIndex = 19 + optLength;
			const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
			const portRemote = new DataView(portBuffer).getUint16(0);

			const addressIndex = portIndex + 2;
			const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));

			const addressType = addressBuffer[0];
			let addressLength = 0;
			let addressValueIndex = addressIndex + 1;
			let addressValue = '';
			switch (addressType) {
				case 1:
					addressLength = 4;
					addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
					break;
				case 2:
					addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
					addressValueIndex += 1;
					addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
					break;
				case 3:
					addressLength = 16;
					const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
					const ipv6 = [];
					for (let i = 0; i < 8; i++) {
						ipv6.push(dataView.getUint16(i * 2).toString(16));
					}
					addressValue = ipv6.join(':');
					break;
				default:
					controller.error(`addressType ${addressType} is not supported`);
			}
			if (!addressValue) {
				controller.error('addressValue is empty');
				return;
			}

			address = addressValue;
			portWithRandomLog = portRemote;
			log(`CONNECT`);
			const rawClientData = vlessBuffer.slice(addressValueIndex + addressLength);

			handleTCPOutBound(remoteSocketWapper, addressValue, portRemote, rawClientData, webSocket, log);
		},
		close() {
			log(`client close context`);
		},
		abort(reason) {
			log(`client abort context`, reason);
		},
	})).catch((err) => {
		log(`readableWebSocketStream pipeTo error`, err);
	});

	return new Response(null, {
		status: 101,
		webSocket: client,
	});
}

async function handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, log) {
	async function connectAndWrite(address, port) {
		const tcpSocket = connect({
			hostname: address,
			port: port,
		});
		remoteSocketWapper.value = tcpSocket;
		log(`connected to ${address}:${port}`);
		const writer = tcpSocket.writable.getWriter();
		await writer.write(rawClientData);
		writer.releaseLock();
		return tcpSocket;
	}

	let rIsBanned = false;
	if (['localhost', '127.0.0.1', '::1'].includes(addressRemote)) {
		rIsBanned = true;
	}
	let serviceSocket = await connectAndWrite(rIsBanned ? proxyIP : addressRemote, rIsBanned ? 80 : portRemote);

	remoteSocketToWS(serviceSocket, webSocket, null, log);
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
	let readableStreamCancel = false;
	const stream = new ReadableStream({
		start(controller) {
			webSocketServer.addEventListener('message', (event) => {
				if (readableStreamCancel) return;
				const message = event.data;
				controller.enqueue(new Uint8Array(message));
			});
			webSocketServer.addEventListener('close', () => {
				safeCloseWebSocket(webSocketServer);
				if (!readableStreamCancel) {
					controller.close();
				}
			});
			webSocketServer.addEventListener('error', (err) => {
				log('webSocketServer has error');
				controller.error(err);
			});
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) {
				controller.error(error);
			} else if (earlyData) {
				controller.enqueue(new Uint8Array(earlyData));
			}
		},
		pull(controller) {},
		cancel(reason) {
			if (readableStreamCancel) return;
			log(`ReadableStream was canceled, reason: ${reason}`);
			readableStreamCancel = true;
			safeCloseWebSocket(webSocketServer);
		}
	});

	return stream;
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, log) {
	let remoteChunkCount = 0;
	let chunks = [];
	let vlessHeader = vlessResponseHeader;
	let hasIncomingData = false;
	await remoteSocket.readable.pipeTo(
		new WritableStream({
			start() {},
			async write(chunk, controller) {
				hasIncomingData = true;
				if (webSocket.readyState !== WebSocket.OPEN) {
					controller.error('webSocket connection is not open');
				}
				if (vlessHeader) {
					webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
					vlessHeader = null;
				} else {
					webSocket.send(chunk);
				}
			},
			close() {
				log(`remoteConnection!.close() called, hasIncomingData ${hasIncomingData}`);
			},
			abort(reason) {
				console.error(`remoteConnection!.abort() called`, reason);
			},
		})
	).catch((err) => {
		log(`remoteSocketToWS has error `, err);
		safeCloseWebSocket(webSocket);
	});
}

function base64ToArrayBuffer(base64Str) {
	if (!base64Str) {
		return { earlyData: null, error: null };
	}
	try {
		base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
		const decode = atob(base64Str);
		const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
		return { earlyData: arryBuffer.buffer, error: null };
	} catch (error) {
		return { earlyData: null, error };
	}
}

function isValidUUID(uuid) {
	const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[40-9a-f]{4}-[89ab0-9a-f]{4}-[0-9a-f]{12}$/i;
	return uuidRegex.test(uuid);
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
	byteToHex.push((i + 0x100).toString(16).slice(1));
}

function stringify(arr, offset = 0) {
	const uuid = (
		byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] +
		byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + '-' +
		byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + '-' +
		byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + '-' +
		byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + '-' +
		byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] +
		byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] +
		byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]
	).toLowerCase();
	return uuid;
}

function safeCloseWebSocket(socket) {
	try {
		if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
			socket.close();
		}
	} catch (error) {
		console.error('safeCloseWebSocket error', error);
	}
}

function makeVLESSConfig(userID, hostName) {ya.ru, 172.66.43.149
	const vlessMain = `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#Cloudflare_VLESS`;
	return `
    <html>
    <head><title>VLESS Config</title><style>body{font-family:sans-serif;background:#1e1e2e;color:#cdd6f4;padding:20px;}code{background:#313244;padding:10px;display:block;word-break:break-all;border-radius:5px;color:#a6e3a1;margin-top:10px;}</style></head>
    <body>
    <h2>Ваш рабочий конфиг VLESS для импорта:</h2>
    <p>Скопируйте строку ниже и импортируйте её в <b>Nekobox</b>, <b>v2rayN</b> или <b>Shadowrocket</b>:</p>
    <code>${vlessMain}</code>
    </body>
    </html>
    `;
}

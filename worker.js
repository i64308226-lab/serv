import { connect } from 'cloudflare:sockets';

// Параметры авторизации
const userID = '9a721389-70dc-4a61-9c31-7b982186541d'; // Твой UUID для VLESS
const USERNAME = "ilya";                               // Логин для Socks5 Telegram
const PASSWORD = "ilya2015";                           // Пароль для Socks5 Telegram

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');
    
    // Если это НЕ WebSocket (проверка ботом или DPI) — прикидываемся сайтом Яндекса
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return fetch(new Request('https://ya.ru', request));
    }

    // Перехватываем WebSocket-дуэли
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    // Запускаем общий парсер (он сам поймет, VLESS это или Socks5)
    handleTrafficBridge(server);

    return new Response(null, { status: 101, webSocketState: client });
  }
};

async function handleTrafficBridge(ws) {
  let tcpSocket = null;
  let isFirstPacket = true;
  let isSocks5Authenticated = false;

  ws.addEventListener('message', async (event) => {
    const buffer = event.data;
    const view = new DataView(buffer);

    // --- ПРОВЕРКА 1: ЕСЛИ ЭТО ЧИСТЫЙ SOCKS5 ОТ TELEGRAM ---
    if (view.getUint8(0) === 0x05 && isFirstPacket && buffer.byteLength <= 5) {
      ws.send(new Uint8Array([0x05, 0x02])); // Отвечаем: нужен логин/пароль
      isFirstPacket = false;
      return;
    }

    // Авторизация Socks5 (ilya / ilya2015)
    if (view.getUint8(0) === 0x01 && !isSocks5Authenticated && !isFirstPacket && tcpSocket === null) {
      const ulen = view.getUint8(1);
      const user = new TextDecoder().decode(buffer.slice(2, 2 + ulen));
      const plen = view.getUint8(2 + ulen);
      const pass = new TextDecoder().decode(buffer.slice(3 + ulen, 3 + ulen + plen));

      if (user === USERNAME && pass === PASSWORD) {
        isSocks5Authenticated = true;
        ws.send(new Uint8Array([0x01, 0x00])); // Успех авторизации Socks5
      } else {
        ws.send(new Uint8Array([0x01, 0x01]));
        ws.close();
      }
      return;
    }

    // Обработка CONNECT запроса от Socks5 Telegram
    if (view.getUint8(0) === 0x05 && view.getUint8(1) === 0x01 && isSocks5Authenticated) {
      const atyp = view.getUint8(3);
      let host = '', port = 0;
      if (atyp === 0x01) {
        host = [view.getUint8(4), view.getUint8(5), view.getUint8(6), view.getUint8(7)].join('.');
        port = view.getUint16(8);
      } else if (atyp === 0x03) {
        const len = view.getUint8(4);
        host = new TextDecoder().decode(buffer.slice(5, 5 + len));
        port = view.getUint16(5 + len);
      }
      
      try {
        tcpSocket = connect({ hostname: host, port: port });
        ws.send(new Uint8Array([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]));
        startPipe(ws, tcpSocket, new Uint8Array());
      } catch { ws.close(); }
      return;
    }

    // --- ПРОВЕРКА 2: ЕСЛИ ЭТО ТРАФИК VLESS ЯНДЕКСА (ОТ КЛИЕНТОВ) ---
    if (isFirstPacket && view.getUint8(0) === 0x00) { 
      isFirstPacket = false;
      const idOffset = 1;
      const idBuffer = buffer.slice(idOffset, idOffset + 16);
      const idHex = Array.from(new Uint8Array(idBuffer)).map(b => b.toString(16).padStart(2, '0')).join('');
      const formattedUUID = `${idHex.slice(0,8)}-${idHex.slice(8,12)}-${idHex.slice(12,16)}-${idHex.slice(16,20)}-${idHex.slice(20)}`;

      if (formattedUUID !== userID) {
        ws.close();
        return;
      }

      const addonLen = view.getUint8(17);
      const portOffset = 18 + addonLen;
      const remotePort = view.getUint16(portOffset);
      const addrType = view.getUint8(portOffset + 2);
      
      let remoteHost = '';
      if (addrType === 1) remoteHost = new Uint8Array(buffer.slice(portOffset + 3, portOffset + 7)).join('.');
      else if (addrType === 3) {
        const hostLen = view.getUint8(portOffset + 3);
        remoteHost = new TextDecoder().decode(buffer.slice(portOffset + 4, portOffset + 4 + hostLen));
      }

      ws.send(new Uint8Array()); // VLESS Handshake response

      try {
        tcpSocket = connect({ hostname: remoteHost, port: remotePort });
        const remainingData = buffer.slice(portOffset + 5);
        startPipe(ws, tcpSocket, remainingData);
      } catch { ws.close(); }
      return;
    }

    // Обычная прогонка данных, если сокет уже открыт
    if (tcpSocket) {
      const writer = tcpSocket.writable.getWriter();
      if (typeof event.data !== 'string') await writer.write(new Uint8Array(event.data));
      writer.releaseLock();
    }
  });
}

function startPipe(ws, socket, initialData) {
  const writer = socket.writable.getWriter();
  if (initialData.byteLength > 0) writer.write(new Uint8Array(initialData));
  writer.releaseLock();

  const reader = socket.readable.getReader();
  (async () => {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        ws.send(value.buffer);
      }
    } catch { ws.close(); }
  })();
}

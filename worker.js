import { connect } from 'cloudflare:sockets'; // Официальный TCP API от Cloudflare

// Конфигурация авторизации
const USERNAME = "ilya";
const PASSWORD = "ilya2015";

export default {
  async fetch(request, env, ctx) {
    // Проверяем, что это запрос на установление WebSocket-соединения от Telegram
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      return new Response('Proxy Node Active', { status: 200 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    handleSocks5Tunnel(server);

    return new Response(null, {
      status: 101,
      webSocketState: client,
    });
  }
};

async function handleSocks5Tunnel(ws) {
  let tcpSocket = null;
  let isAuthenticated = false;

  ws.addEventListener('message', async (event) => {
    const buffer = event.data;
    const view = new DataView(buffer);

    // Этап 1: Рукопожатие SOCKS5 (выбор метода авторизации)
    if (view.getUint8(0) === 0x05 && !isAuthenticated && buffer.byteLength <= 5) {
      ws.send(new Uint8Array([0x05, 0x02])); // 0x02 означает: Требуется Логин/Пароль
      return;
    }

    // Этап 2: Проверка Логина и Пароля (ilya / ilya2015)
    if (view.getUint8(0) === 0x01 && !isAuthenticated) {
      const ulen = view.getUint8(1);
      const usernameInRequest = new TextDecoder().decode(buffer.slice(2, 2 + ulen));
      
      const plen = view.getUint8(2 + ulen);
      const passwordInRequest = new TextDecoder().decode(buffer.slice(3 + ulen, 3 + ulen + plen));

      if (usernameInRequest === USERNAME && passwordInRequest === PASSWORD) {
        isAuthenticated = true;
        ws.send(new Uint8Array([0x01, 0x00])); // 0x00 — Успешно, доступ разрешен
      } else {
        ws.send(new Uint8Array([0x01, 0x01])); // 0x01 — Ошибка авторизации
        ws.close();
      }
      return;
    }

    // Этап 3: Пересылка пакетов (CONNECT к серверам Telegram)
    if (view.getUint8(0) === 0x05 && view.getUint8(1) === 0x01 && isAuthenticated) {
      const atyp = view.getUint8(3);
      let address = '';
      let port = 0;

      if (atyp === 0x01) { // IPv4 адрес
        address = [view.getUint8(4), view.getUint8(5), view.getUint8(6), view.getUint8(7)].join('.');
        port = view.getUint16(8);
      } else if (atyp === 0x03) { // Доменное имя
        const len = view.getUint8(4);
        address = new TextDecoder().decode(buffer.slice(5, 5 + len));
        port = view.getUint16(5 + len);
      }

      try {
        // Подключаемся к серверам мессенджера из инфраструктуры Cloudflare
        tcpSocket = connect({ hostname: address, port: port }); 
        
        ws.send(new Uint8Array([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]));
        pipe(ws, tcpSocket);
      } catch (err) {
        ws.send(new Uint8Array([0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0]));
        ws.close();
      }
    }
  });
}

function pipe(ws, socket) {
  const writer = socket.writable.getWriter();
  
  ws.addEventListener('message', async (e) => {
    if (typeof e.data !== 'string') {
      await writer.write(new Uint8Array(e.data));
    }
  });

  const reader = socket.readable.getReader();
  (async () => {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        ws.send(value.buffer);
      }
    } catch (e) {
      ws.close();
    }
  })();
}

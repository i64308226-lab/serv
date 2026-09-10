import { connect } from 'cloudflare:sockets';

const USERNAME = "ilya";
const PASSWORD = "ilya2015";

export default {
  async fetch(request, env, ctx) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader !== 'websocket') {
      // Имитируем главную страницу Яндекса, если кто-то зайдет через браузер
      return fetch(new Request('https://ya.ru', request));
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();
    
    handleSocks5(server);
    return new Response(null, { status: 101, webSocketState: client });
  }
};

async function handleSocks5(ws) {
  let tcpSocket = null;
  let authenticated = false;

  ws.addEventListener('message', async (event) => {
    const buffer = event.data;
    const view = new DataView(buffer);

    // 1. Рукопожатие
    if (view.getUint8(0) === 0x05 && !authenticated && buffer.byteLength <= 5) {
      ws.send(new Uint8Array([0x05, 0x02])); // Требуем логин/пароль
      return;
    }

    // 2. Авторизация ilya / ilya2015
    if (view.getUint8(0) === 0x01 && !authenticated) {
      const ulen = view.getUint8(1);
      const user = new TextDecoder().decode(buffer.slice(2, 2 + ulen));
      const plen = view.getUint8(2 + ulen);
      const pass = new TextDecoder().decode(buffer.slice(3 + ulen, 3 + ulen + plen));

      if (user === USERNAME && pass === PASSWORD) {
        authenticated = true;
        ws.send(new Uint8Array([0x01, 0x00])); // Успех
      } else {
        ws.send(new Uint8Array([0x01, 0x01]));
        ws.close();
      }
      return;
    }

    // 3. Подключение к Telegram
    if (view.getUint8(0) === 0x05 && view.getUint8(1) === 0x01 && authenticated) {
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
        
        // Пересылка потоков
        const writer = tcpSocket.writable.getWriter();
        ws.addEventListener('message', async (e) => {
          if (typeof e.data !== 'string') await writer.write(new Uint8Array(e.data));
        });

        const reader = tcpSocket.readable.getReader();
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          ws.send(value.buffer);
        }
      } catch {
        ws.close();
      }
    }
  });
}

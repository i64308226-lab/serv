export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    // Перенаправляем все запросы на реальные сервера Google/YouTube
    const targetUrl = 'https://' + url.host + url.pathname + url.search;
    
    const newRequest = new Request(targetUrl, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: 'manual'
    });

    try {
      return await fetch(newRequest);
    } catch (e) {
      return new Response('Proxy Error: ' + e.message, { status: 500 });
    }
  }
};

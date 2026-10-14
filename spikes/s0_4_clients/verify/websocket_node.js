// S0.4 verification: ws (Node) full local echo round-trip (server + client).
// Run: npm init -y && npm install ws && node websocket_node.js
const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: 18180 });
wss.on('connection', (s) => s.on('message', (m) => s.send('echo:' + m)));
const c = new WebSocket('ws://127.0.0.1:18180');
c.on('open', () => c.send('hello'));
c.on('message', (m) => {
  console.log('client received:', m.toString());
  wss.close();
  c.close();
  process.exit(0);
});
setTimeout(() => { console.log('TIMEOUT'); process.exit(1); }, 5000);

// Test-only fixture: listens on loopback, ephemeral ports, synthetic key/data only.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const root = process.argv[2];
const ports = [];
const waiting = [];
function event(text, finish = null) {
  return `data: ${JSON.stringify({choices:[{index:0,delta:{content:text},finish_reason:finish}]})}\n\n`;
}
for (let i=0;i<2;i++) {
  const server = http.createServer((req,res)=>{
    let body=''; req.on('data',c=>body+=c); req.on('end',()=>{
      let mode=req.url.slice(1);
      if(req.url==='/v1/chat/completions') mode=JSON.parse(body).messages.at(-1).content;
      fs.appendFileSync(path.join(root,'requests.jsonl'),JSON.stringify({node:i,mode,time:Date.now()})+'\n');
      if(req.headers.authorization!=='Bearer fixture-key'){res.writeHead(401,{'Content-Type':'application/json'});res.end('{}');return;}
      if(mode==='headers-stall') return;
      if(mode==='redirect'){res.writeHead(302,{Location:`http://127.0.0.1:${ports[1-i]}/redirect-target`});res.end();return;}
      if(mode==='busy'){res.writeHead(503,{'Content-Type':'application/json'});res.end('{}');return;}
      if(mode==='health'){res.writeHead(200,{'Content-Type':'application/json'});res.end('{"status":"ok"}');return;}
      res.writeHead(200,{'Content-Type':'text/event-stream; charset=utf-8','Cache-Control':'no-cache'});res.flushHeaders();
      if(mode==='body-stall') return;
      if(mode==='invalid-utf8'){res.end(Buffer.from([0xff,0xfe,0xff]));return;}
      if(mode==='trickle'){const t=setInterval(()=>res.write(': heartbeat\n\n'),100);res.on('close',()=>clearInterval(t));return;}
      if(mode==='concurrent') {
        waiting.push({res,i});
        if(waiting.length===2){
          fs.writeFileSync(path.join(root,'overlap'),'both HTTP requests active');
          for(const item of waiting){item.res.write(event(`node-${item.i}`));setTimeout(()=>item.res.end(event(' done','stop')+'data: [DONE]\n\n'),200);}
        }
        return;
      }
      res.write(event(`node-${i}: café 中 🌱`));
      if(mode==='drop'){setTimeout(()=>res.socket.destroy(),250);return;}
      // Byte-by-byte HTTP chunks split multibyte UTF-8 code points and SSE delimiters.
      const tail=Buffer.from(event(' second 中🌱','stop')+'data: [DONE]\n\n');let n=0;
      const t=setInterval(()=>{if(n<tail.length)res.write(tail.subarray(n,n+++1));else{clearInterval(t);res.end();}},5);
      res.on('close',()=>clearInterval(t));
    });
  });
  server.listen(0,'127.0.0.1',()=>{ports[i]=server.address().port;if(ports.filter(Boolean).length===2)fs.writeFileSync(path.join(root,'ready.json'),JSON.stringify(ports));});
}

const base='http://localhost:3333';

async function getJson(url){
  const r = await fetch(url);
  const t = await r.text();
  let j;
  try { j = JSON.parse(t); } catch { throw new Error(`Non-JSON from ${url}: ${t.slice(0,200)}`); }
  if (!r.ok) throw new Error(`${url} -> ${r.status} ${JSON.stringify(j).slice(0,200)}`);
  return j;
}

(async () => {
  const health = await getJson(base + '/health');
  console.log('health:', { bridge: health.bridge, groups: health.groups });

  const groups = await getJson(base + '/groups');
  console.log('groups:', groups.map(g => `${g.id}:${g.env || 'missing'}`).join(', '));

  const srcGroup='manhattan-main', tgtGroup='manhattan-main';
  const srcSchema='SE_DM', tgtSchema='FE_DM';

  const srcSchemas = await getJson(`${base}/db/schemas?group=${srcGroup}`);
  const tgtSchemas = await getJson(`${base}/db/schemas?group=${tgtGroup}`);
  const hasSrc = srcSchemas.some(r => (r.OWNER || r) === srcSchema);
  const hasTgt = tgtSchemas.some(r => (r.OWNER || r) === tgtSchema);
  console.log('workflow-a-schemas:', { hasSrc, hasTgt });

  const srcObjs = await getJson(`${base}/db/objects?group=${srcGroup}&schema=${srcSchema}`);
  const tgtObjs = await getJson(`${base}/db/objects?group=${tgtGroup}&schema=${tgtSchema}`);
  console.log('workflow-a-objects:', { src: srcObjs.length, tgt: tgtObjs.length });

  const crossSrc='manhattan-main', crossTgt='manhattan-ck', crossSchema='MANH';
  const crossSrcObjs = await getJson(`${base}/db/objects?group=${crossSrc}&schema=${crossSchema}`);
  const crossTgtObjs = await getJson(`${base}/db/objects?group=${crossTgt}&schema=${crossSchema}`);
  console.log('workflow-b-proxy-cross-group:', { src: crossSrcObjs.length, tgt: crossTgtObjs.length });

  const sample = srcObjs.find(o => (o.OBJECT_TYPE||o.TYPE)==='TABLE') || srcObjs[0];
  if (sample) {
    const name = sample.OBJECT_NAME || sample.NAME;
    const type = sample.OBJECT_TYPE || sample.TYPE;
    if (type === 'TABLE') {
      const cols = await getJson(`${base}/db/columns?group=${srcGroup}&schema=${srcSchema}&table=${encodeURIComponent(name)}`);
      console.log('detail-columns-ok:', { table: name, count: cols.length });
    } else {
      const src = await getJson(`${base}/db/source?group=${srcGroup}&schema=${srcSchema}&name=${encodeURIComponent(name)}&type=${encodeURIComponent(type)}`);
      console.log('detail-source-ok:', { object: name, type, lines: src.length });
    }
  }

  console.log('SMOKE_OK');
})().catch(e => {
  console.error('SMOKE_FAIL', e.message);
  process.exit(1);
});

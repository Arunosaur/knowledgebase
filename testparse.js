function parseMCPResult(response) {
  const text = (response?.result?.content || [])
    .filter(c => c && c.type === 'text')
    .map(c => c.text || '')
    .join('\n');
  let lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  lines = lines.filter(l => !/^#+/.test(l));
  const headerIdx = lines.findIndex(l => /OBJECT_NAME|COLUMN_NAME|NAME|OWNER/.test(l));
  console.log('lines count', lines.length, 'headerIdx', headerIdx);
  console.log('first 5 lines', lines.slice(0,5));
  if (headerIdx === -1) return [];
  if (headerIdx > 0) lines = lines.slice(headerIdx);
  if (lines.length < 2) return [];
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  return lines.slice(1).map(line => {
    const cols = line.match(/("(?:[^"]|"" )*"|[^,]*)/g)
      .map(c => c.trim().replace(/^"|"$/g, '').replace(/""/g, '"'));
    return Object.fromEntries(headers.map((h,i)=>[h,cols[i]||'']));
  });
}

const sample = `"OBJECT_NAME","OBJECT_TYPE","STATUS"
"ACCEP_STATUS_PK","INDEX","VALID"
"ACCESSORIAL_PK","INDEX","VALID"
"ACCESSORIAL_CODE","SYNONYM","VALID"
"ACCESSORIAL_GROUP","SYNONYM","VALID"
"ABW_ANALYSIS_RESULTS","TABLE","VALID"`;
console.log(parseMCPResult({result:{content:[{type:'text',text:sample}]}}));

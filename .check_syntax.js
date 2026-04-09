#!/usr/bin/env node
const vm = require('vm');
const fs = require('fs');

const html = fs.readFileSync('/Users/asrajag/Workspace/oracle/knowledgeBase/public/index.html', 'utf8');
const scriptMatch = html.match(/<script>([\s\S]*)<\/script>/);

if(!scriptMatch){
  console.log('✗ No script tag found');
  process.exit(1);
}

const code = scriptMatch[1];
try {
  new vm.Script(code);
  console.log('✓ PARSE_OK - JavaScript syntax is valid');
  process.exit(0);
} catch(e){
  console.log('✗ PARSE_ERROR:', e.message);
  const lines = code.split('\n');
  const match = e.stack.match(/:(\d+):/);
  const errLine = match ? parseInt(match[1]) : 0;
  if(errLine && lines[errLine-1]){
    console.log(`Line ${errLine}: `, lines[errLine-1].slice(0, 120));
    if(lines[errLine]) console.log(`Line ${errLine+1}: `, lines[errLine].slice(0, 120));
  }
  process.exit(1);
}

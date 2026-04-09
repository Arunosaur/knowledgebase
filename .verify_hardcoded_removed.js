const fs = require('fs');
const html = fs.readFileSync('/Users/asrajag/Workspace/oracle/knowledgeBase/public/index.html', 'utf8');
const hasOldDetector = html.includes('function isShipmentMaxUnitsQuestion');
const hasOldHandler = html.includes('if(liveMaxUnitsLookup');
const hasSemanticSystem = html.includes('resolveSemanticContext');

console.log('✓ Old detector removed:', !hasOldDetector);
console.log('✓ Old handler removed:', !hasOldHandler);
console.log('✓ Semantic system present:', hasSemanticSystem);
process.exit(!hasOldDetector && !hasOldHandler ? 0 : 1);

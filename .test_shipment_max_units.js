#!/usr/bin/env node
const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  
  await page.goto('file:///Users/asrajag/Workspace/oracle/knowledgeBase/public/index.html', { waitUntil: 'networkidle' });
  
  // Wait for Ask mode button
  await page.waitForSelector('text=Ask a Question', { timeout: 10000 });
  
  // Click Ask a Question
  await page.click('text=Ask a Question');
  await page.waitForTimeout(2000);
  
  // Type the question
  const question = 'Which load is shipping the most units for MZ?';
  const inputSelector = 'input[type="text"][placeholder*="What"]';
  await page.fill(inputSelector, question);
  
  // Click Ask button
  await page.click('button:has-text("Ask")');
  
  // Wait for the answer to load
  await page.waitForTimeout(3000);
  
  // Check if routing shows the expected pattern
  const routingText = await page.textContent('[id*="business-routing"]') || await page.textContent('[id*="routing"]') || '';
  const answerText = await page.textContent('[id*="business-answer"]') || await page.textContent('[id*="answer"]') || '';
  
  console.log('Question:', question);
  console.log('Routing:', routingText.slice(0, 150));
  console.log('Answer:', answerText.slice(0, 200));
  
  // Check if it's using deterministic database path (not AI speculation)
  const hasDbPath = routingText.includes('Manhattan database') || routingText.includes('Querying') || answerText.includes('shipment');
  const hasSpeculation = answerText.includes('Based on') || answerText.includes('According to knowledge');
  
  console.log('\n✓ Has DB routing:', hasDbPath);
  console.log('✓ Avoids speculation:', !hasSpeculation);
  
  await browser.close();
  process.exit(hasDbPath && !hasSpeculation ? 0 : 1);
})().catch(e => {
  console.error('Test error:', e.message);
  process.exit(1);
});

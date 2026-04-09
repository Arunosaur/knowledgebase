const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const APP_URL = process.env.MATRIX_APP_URL || 'http://localhost:3333';
const OUTPUT_FILE = process.env.MATRIX_OUTPUT_FILE || path.join(process.cwd(), 'ask_anytype_matrix_results.json');
const HEADLESS = process.env.MATRIX_HEADLESS !== 'false';

const INTERIM_HINTS = [
  'starting analysis',
  'collecting context',
  'generating business answer',
  'querying',
  'searching',
  'running live data',
  'phase:',
  'connecting to wms'
];

async function launchBrowser() {
  const launchOptions = { headless: HEADLESS };
  const requestedChannel = process.env.MATRIX_BROWSER_CHANNEL;

  if (requestedChannel) {
    return chromium.launch({ ...launchOptions, channel: requestedChannel });
  }

  try {
    return await chromium.launch(launchOptions);
  } catch (err) {
    const msg = String(err && err.message ? err.message : err);
    if (!/Executable doesn't exist/i.test(msg)) {
      throw err;
    }
  }

  const fallbackChannels = ['chrome', 'msedge'];
  for (const channel of fallbackChannels) {
    try {
      return await chromium.launch({ ...launchOptions, channel });
    } catch (_) {
    }
  }

  throw new Error('No runnable Playwright browser found. Run `npx playwright install` or set MATRIX_BROWSER_CHANNEL.');
}

const QUESTIONS = [
  { type: 'operational-no-dc', q: 'how many shipments were closed in the last 4 hours?' },
  { type: 'operational-with-dc', q: 'how many shipments were closed in the last 4 hours for SE DC?' },
  { type: 'issue', q: 'are there any issues related to MZIC6101 variance?' },
  { type: 'jira-ownership', q: 'what was last issue assigned to wmshub group?' },
  { type: 'process', q: 'how does receiving process work in WMS?' },
  { type: 'concept', q: 'why is Manhattan needed for retail grocery?' }
];

async function waitForAnswer(page, previousAnswer = '', timeoutMs = 28000) {
  const start = Date.now();
  let candidate = '';
  let candidateSince = 0;
  while (Date.now() - start < timeoutMs) {
    const txt = ((await page.locator('#business-answer').innerText().catch(() => '')) || '').trim();
    const changed = txt && txt !== String(previousAnswer || '').trim();
    const normalized = txt.toLowerCase();
    const isInterim = INTERIM_HINTS.some(h => normalized.includes(h));
    if (changed && !isInterim) {
      if (txt !== candidate) {
        candidate = txt;
        candidateSince = Date.now();
      }
      if (Date.now() - candidateSince >= 2200) {
        return candidate;
      }
    }
    await page.waitForTimeout(700);
  }
  return candidate || ((await page.locator('#business-answer').innerText().catch(() => '')) || '').trim();
}

(async () => {
  const browser = await launchBrowser();
  const results = [];

  for (const item of QUESTIONS) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 960 } });
    await page.goto(APP_URL, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await page.locator(".mode-tile:has-text('Ask a Question')").first().click();

    await page.fill('#business-q', item.q);
    await page.click('#business-ask-btn');

    let answer = await waitForAnswer(page, '', 36000);
    if (!answer) {
      await page.fill('#business-q', item.q);
      await page.click('#business-ask-btn');
      answer = await waitForAnswer(page, '', 36000);
    }
    const routing = ((await page.locator('#business-routing').innerText().catch(() => '')) || '').trim();

    const hardStopDcPrompt = /which dc should i use/i.test(answer);
    const hasError = /^⚠/.test(answer);
    const hasAnswer = answer.length > 0;
    const pass = hasAnswer && !hardStopDcPrompt && !hasError;

    results.push({
      type: item.type,
      question: item.q,
      pass,
      hasAnswer,
      hardStopDcPrompt,
      hasError,
      routing: routing.slice(0, 300),
      answerPreview: answer.slice(0, 380)
    });

    await page.close();
  }

  await browser.close();

  const summary = {
    ok: results.every(r => r.pass),
    passed: results.filter(r => r.pass).length,
    total: results.length,
    results
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify({ ok: summary.ok, passed: summary.passed, total: summary.total }));
})();

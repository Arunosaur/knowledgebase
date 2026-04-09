const { test, describe, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const http = require('http');

// Use a temporary articles directory for tests
const TEST_ARTICLES_DIR = path.join(__dirname, '../tmp-test-articles');

// Patch the server to use the test articles dir before loading
process.env.ARTICLES_DIR = TEST_ARTICLES_DIR;

const app = require('../server');

let server;
let baseUrl;

before(async () => {
  // Ensure test articles directory exists and is empty
  fs.rmSync(TEST_ARTICLES_DIR, { recursive: true, force: true });
  fs.mkdirSync(TEST_ARTICLES_DIR, { recursive: true });

  server = http.createServer(app);
  await new Promise(resolve => server.listen(0, resolve));
  baseUrl = `http://localhost:${server.address().port}`;
});

after(async () => {
  await new Promise(resolve => server.close(resolve));
  fs.rmSync(TEST_ARTICLES_DIR, { recursive: true, force: true });
});

beforeEach(() => {
  // Clean articles between tests
  if (fs.existsSync(TEST_ARTICLES_DIR)) {
    fs.readdirSync(TEST_ARTICLES_DIR).forEach(f => {
      fs.unlinkSync(path.join(TEST_ARTICLES_DIR, f));
    });
  }
});

async function request(method, url, body) {
  return new Promise((resolve, reject) => {
    const opts = {
      method,
      headers: { 'Content-Type': 'application/json' },
    };
    const req = http.request(baseUrl + url, opts, res => {
      let data = '';
      res.on('data', chunk => (data += chunk));
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(data); } catch { parsed = data; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

describe('GET /api/articles', () => {
  test('returns empty array when no articles', async () => {
    const { status, body } = await request('GET', '/api/articles');
    assert.equal(status, 200);
    assert.deepEqual(body, []);
  });

  test('returns list of articles after creating one', async () => {
    await request('POST', '/api/articles', {
      slug: 'test-article',
      title: 'Test Article',
      category: 'Test',
      tags: ['tag1'],
      body: 'Hello world',
    });
    const { status, body } = await request('GET', '/api/articles');
    assert.equal(status, 200);
    assert.equal(body.length, 1);
    assert.equal(body[0].slug, 'test-article');
    assert.equal(body[0].title, 'Test Article');
  });

  test('filters by search query', async () => {
    await request('POST', '/api/articles', { slug: 'alpha', title: 'Alpha Article', category: 'A', tags: [], body: 'unique content here' });
    await request('POST', '/api/articles', { slug: 'beta', title: 'Beta Article', category: 'B', tags: [], body: 'other content' });

    const { body } = await request('GET', '/api/articles?q=unique');
    assert.equal(body.length, 1);
    assert.equal(body[0].slug, 'alpha');
  });

  test('filters by tag', async () => {
    await request('POST', '/api/articles', { slug: 'tagged', title: 'Tagged', category: 'X', tags: ['special'], body: 'body' });
    await request('POST', '/api/articles', { slug: 'untagged', title: 'Untagged', category: 'X', tags: [], body: 'body' });

    const { body } = await request('GET', '/api/articles?tag=special');
    assert.equal(body.length, 1);
    assert.equal(body[0].slug, 'tagged');
  });

  test('filters by category', async () => {
    await request('POST', '/api/articles', { slug: 'cat-a', title: 'Cat A', category: 'Docs', tags: [], body: 'b' });
    await request('POST', '/api/articles', { slug: 'cat-b', title: 'Cat B', category: 'Other', tags: [], body: 'b' });

    const { body } = await request('GET', '/api/articles?category=Docs');
    assert.equal(body.length, 1);
    assert.equal(body[0].slug, 'cat-a');
  });
});

describe('POST /api/articles', () => {
  test('creates a new article', async () => {
    const { status, body } = await request('POST', '/api/articles', {
      slug: 'new-article',
      title: 'New Article',
      category: 'General',
      tags: ['test'],
      body: '# Hello',
    });
    assert.equal(status, 201);
    assert.equal(body.slug, 'new-article');
    assert.equal(body.title, 'New Article');
    assert.equal(body.category, 'General');
    assert.deepEqual(body.tags, ['test']);
    assert.ok(body.html.includes('<h1>'));
  });

  test('returns 400 when slug is missing', async () => {
    const { status, body } = await request('POST', '/api/articles', { title: 'No Slug' });
    assert.equal(status, 400);
    assert.ok(body.error);
  });

  test('returns 400 for invalid slug format', async () => {
    const { status, body } = await request('POST', '/api/articles', {
      slug: 'Invalid Slug!',
      title: 'Test',
    });
    assert.equal(status, 400);
    assert.ok(body.error);
  });

  test('returns 409 when article already exists', async () => {
    await request('POST', '/api/articles', { slug: 'duplicate', title: 'Dup', body: '' });
    const { status, body } = await request('POST', '/api/articles', { slug: 'duplicate', title: 'Dup2', body: '' });
    assert.equal(status, 409);
    assert.ok(body.error);
  });
});

describe('GET /api/articles/:slug', () => {
  test('returns article by slug', async () => {
    await request('POST', '/api/articles', { slug: 'my-article', title: 'My Article', category: 'Docs', tags: ['a', 'b'], body: '# Content' });

    const { status, body } = await request('GET', '/api/articles/my-article');
    assert.equal(status, 200);
    assert.equal(body.slug, 'my-article');
    assert.equal(body.title, 'My Article');
    assert.ok(body.html);
  });

  test('returns 404 for missing article', async () => {
    const { status } = await request('GET', '/api/articles/does-not-exist');
    assert.equal(status, 404);
  });

  test('returns 400 for invalid slug', async () => {
    const { status } = await request('GET', '/api/articles/Invalid%20Slug!');
    assert.equal(status, 400);
  });
});

describe('PUT /api/articles/:slug', () => {
  test('updates an existing article', async () => {
    await request('POST', '/api/articles', { slug: 'editable', title: 'Original', category: 'A', tags: ['old'], body: 'old body' });

    const { status, body } = await request('PUT', '/api/articles/editable', {
      title: 'Updated',
      category: 'B',
      tags: ['new'],
      body: 'new body',
    });
    assert.equal(status, 200);
    assert.equal(body.title, 'Updated');
    assert.equal(body.category, 'B');
    assert.deepEqual(body.tags, ['new']);
    assert.equal(body.body, 'new body');
  });

  test('returns 404 when article does not exist', async () => {
    const { status } = await request('PUT', '/api/articles/nonexistent', { title: 'X' });
    assert.equal(status, 404);
  });
});

describe('DELETE /api/articles/:slug', () => {
  test('deletes an existing article', async () => {
    await request('POST', '/api/articles', { slug: 'to-delete', title: 'Delete Me', body: '' });

    const { status, body } = await request('DELETE', '/api/articles/to-delete');
    assert.equal(status, 200);
    assert.ok(body.message);

    const { status: getStatus } = await request('GET', '/api/articles/to-delete');
    assert.equal(getStatus, 404);
  });

  test('returns 404 for non-existent article', async () => {
    const { status } = await request('DELETE', '/api/articles/ghost-article');
    assert.equal(status, 404);
  });
});

describe('GET /api/tags', () => {
  test('returns all unique tags sorted', async () => {
    await request('POST', '/api/articles', { slug: 'art1', title: 'A1', tags: ['zebra', 'alpha'], body: '' });
    await request('POST', '/api/articles', { slug: 'art2', title: 'A2', tags: ['alpha', 'beta'], body: '' });

    const { status, body } = await request('GET', '/api/tags');
    assert.equal(status, 200);
    assert.deepEqual(body, ['alpha', 'beta', 'zebra']);
  });
});

describe('GET /api/categories', () => {
  test('returns all unique categories sorted', async () => {
    await request('POST', '/api/articles', { slug: 'c1', title: 'C1', category: 'Zebra', tags: [], body: '' });
    await request('POST', '/api/articles', { slug: 'c2', title: 'C2', category: 'Alpha', tags: [], body: '' });

    const { status, body } = await request('GET', '/api/categories');
    assert.equal(status, 200);
    assert.deepEqual(body, ['Alpha', 'Zebra']);
  });
});

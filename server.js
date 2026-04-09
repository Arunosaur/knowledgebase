const express = require('express');
const fs = require('fs');
const path = require('path');
const { marked } = require('marked');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const ARTICLES_DIR = path.resolve(process.env.ARTICLES_DIR || path.join(__dirname, 'articles'));

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Rate limiter for write operations (create, update, delete)
const writeLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' },
});

// Helper: resolve article file path, confined to ARTICLES_DIR
function articlePath(slug) {
  const resolved = path.resolve(ARTICLES_DIR, `${slug}.md`);
  if (!resolved.startsWith(ARTICLES_DIR + path.sep) && resolved !== ARTICLES_DIR) {
    return null;
  }
  return resolved;
}

// Helper: list all article slugs
function listSlugs() {
  if (!fs.existsSync(ARTICLES_DIR)) return [];
  return fs.readdirSync(ARTICLES_DIR)
    .filter(f => f.endsWith('.md'))
    .map(f => f.replace(/\.md$/, ''));
}

// Helper: read and parse an article file
function readArticle(slug) {
  const filePath = articlePath(slug);
  if (!filePath || !fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath, 'utf8');
  return parseArticle(slug, raw);
}

// Helper: parse frontmatter + body from markdown
function parseArticle(slug, raw) {
  let title = slug;
  let tags = [];
  let category = 'General';
  let body = raw;

  const fmMatch = raw.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (fmMatch) {
    const fm = fmMatch[1];
    body = fmMatch[2];
    const titleMatch = fm.match(/^title:\s*(.+)$/m);
    const tagsMatch = fm.match(/^tags:\s*(.+)$/m);
    const categoryMatch = fm.match(/^category:\s*(.+)$/m);
    if (titleMatch) title = titleMatch[1].trim();
    if (tagsMatch) tags = tagsMatch[1].split(',').map(t => t.trim()).filter(Boolean);
    if (categoryMatch) category = categoryMatch[1].trim();
  }

  return { slug, title, tags, category, body, html: marked(body) };
}

// Helper: write article to disk
function writeArticle(slug, title, category, tags, body) {
  const filePath = articlePath(slug);
  if (!filePath) throw new Error('Invalid slug');
  const fm = `---\ntitle: ${title}\ncategory: ${category}\ntags: ${tags.join(', ')}\n---\n`;
  fs.writeFileSync(filePath, fm + body, 'utf8');
}

// Helper: validate slug (alphanumeric and hyphens only)
function isValidSlug(slug) {
  return /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(slug);
}

// GET /api/articles - list all articles (with optional search and tag filter)
app.get('/api/articles', (req, res) => {
  const { q, tag, category } = req.query;
  const slugs = listSlugs();
  let articles = slugs.map(readArticle).filter(Boolean);

  if (q) {
    const query = q.toLowerCase();
    articles = articles.filter(a =>
      a.title.toLowerCase().includes(query) ||
      a.body.toLowerCase().includes(query) ||
      a.tags.some(t => t.toLowerCase().includes(query))
    );
  }
  if (tag) {
    articles = articles.filter(a => a.tags.map(t => t.toLowerCase()).includes(tag.toLowerCase()));
  }
  if (category) {
    articles = articles.filter(a => a.category.toLowerCase() === category.toLowerCase());
  }

  res.json(articles.map(({ slug, title, tags, category }) => ({ slug, title, tags, category })));
});

// GET /api/articles/:slug - get a single article
app.get('/api/articles/:slug', (req, res) => {
  const { slug } = req.params;
  if (!isValidSlug(slug)) return res.status(400).json({ error: 'Invalid slug' });
  const article = readArticle(slug);
  if (!article) return res.status(404).json({ error: 'Article not found' });
  res.json(article);
});

// POST /api/articles - create a new article
app.post('/api/articles', writeLimiter, (req, res) => {
  const { slug, title, category = 'General', tags = [], body = '' } = req.body;
  if (!slug || !title) return res.status(400).json({ error: 'slug and title are required' });
  if (!isValidSlug(slug)) return res.status(400).json({ error: 'Invalid slug: use lowercase letters, numbers and hyphens only' });

  const filePath = articlePath(slug);
  if (!filePath) return res.status(400).json({ error: 'Invalid slug' });
  if (fs.existsSync(filePath)) return res.status(409).json({ error: 'Article already exists' });

  writeArticle(slug, title, category, tags, body);
  res.status(201).json(readArticle(slug));
});

// PUT /api/articles/:slug - update an existing article
app.put('/api/articles/:slug', writeLimiter, (req, res) => {
  const { slug } = req.params;
  if (!isValidSlug(slug)) return res.status(400).json({ error: 'Invalid slug' });

  const current = readArticle(slug);
  if (!current) return res.status(404).json({ error: 'Article not found' });

  const {
    title = current.title,
    category = current.category,
    tags = current.tags,
    body = current.body
  } = req.body;

  writeArticle(slug, title, category, tags, body);
  res.json(readArticle(slug));
});

// DELETE /api/articles/:slug - delete an article
app.delete('/api/articles/:slug', writeLimiter, (req, res) => {
  const { slug } = req.params;
  if (!isValidSlug(slug)) return res.status(400).json({ error: 'Invalid slug' });
  const filePath = articlePath(slug);
  if (!filePath || !fs.existsSync(filePath)) return res.status(404).json({ error: 'Article not found' });
  fs.unlinkSync(filePath);
  res.json({ message: 'Article deleted' });
});

// GET /api/tags - list all unique tags
app.get('/api/tags', (req, res) => {
  const slugs = listSlugs();
  const tagSet = new Set();
  slugs.forEach(slug => {
    const a = readArticle(slug);
    if (a) a.tags.forEach(t => tagSet.add(t));
  });
  res.json([...tagSet].sort());
});

// GET /api/categories - list all unique categories
app.get('/api/categories', (req, res) => {
  const slugs = listSlugs();
  const catSet = new Set();
  slugs.forEach(slug => {
    const a = readArticle(slug);
    if (a) catSet.add(a.category);
  });
  res.json([...catSet].sort());
});

// Serve the SPA for all other routes
app.get('/{*splat}', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Knowledgebase running at http://localhost:${PORT}`);
  });
}

module.exports = app;

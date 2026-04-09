/* global state */
let allArticles = [];
let currentSlug = null;
let editingSlug = null;
let activeCategory = null;
let activeTag = null;
let searchQuery = '';

/* ===== DOM refs ===== */
const articleListView   = document.getElementById('article-list-view');
const articleDetailView = document.getElementById('article-detail-view');
const articleEditor     = document.getElementById('article-editor');
const articleCards      = document.getElementById('article-cards');
const noResults         = document.getElementById('no-results');
const listTitle         = document.getElementById('list-title');
const articleCount      = document.getElementById('article-count');
const articleContent    = document.getElementById('article-content');
const searchInput       = document.getElementById('search-input');
const categoryList      = document.getElementById('category-list');
const tagList           = document.getElementById('tag-list');
const categoryDatalist  = document.getElementById('category-datalist');

/* ===== API helpers ===== */
async function apiFetch(url, options = {}) {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'Request failed');
  return data;
}

/* ===== Navigation ===== */
function showListView() {
  articleListView.classList.remove('hidden');
  articleDetailView.classList.add('hidden');
  articleEditor.classList.add('hidden');
  currentSlug = null;
  editingSlug = null;
}

function showDetailView() {
  articleListView.classList.add('hidden');
  articleDetailView.classList.remove('hidden');
  articleEditor.classList.add('hidden');
}

function showEditorView() {
  articleListView.classList.add('hidden');
  articleDetailView.classList.add('hidden');
  articleEditor.classList.remove('hidden');
}

/* ===== Render helpers ===== */
function renderTagPill(tag, active = false) {
  const span = document.createElement('span');
  span.className = 'tag-pill' + (active ? ' active' : '');
  span.textContent = tag;
  span.title = `Filter by tag: ${tag}`;
  span.addEventListener('click', () => toggleTagFilter(tag));
  return span;
}

function renderArticleCard(article) {
  const div = document.createElement('div');
  div.className = 'article-card';
  div.setAttribute('role', 'button');
  div.setAttribute('tabindex', '0');
  div.setAttribute('aria-label', `Open article: ${article.title}`);
  div.innerHTML = `
    <div class="article-card-title">${escapeHtml(article.title)}</div>
    <div class="article-card-category">📁 ${escapeHtml(article.category)}</div>
    <div class="article-card-tags"></div>
  `;
  const tagContainer = div.querySelector('.article-card-tags');
  article.tags.forEach(tag => tagContainer.appendChild(renderTagPill(tag)));
  div.addEventListener('click', () => openArticle(article.slug));
  div.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') openArticle(article.slug); });
  return div;
}

function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ===== Article list ===== */
async function loadArticleList() {
  const params = new URLSearchParams();
  if (searchQuery) params.set('q', searchQuery);
  if (activeTag) params.set('tag', activeTag);
  if (activeCategory) params.set('category', activeCategory);

  const url = '/api/articles' + (params.toString() ? '?' + params : '');
  allArticles = await apiFetch(url);

  articleCards.innerHTML = '';
  articleCount.textContent = `(${allArticles.length})`;

  if (allArticles.length === 0) {
    noResults.classList.remove('hidden');
  } else {
    noResults.classList.add('hidden');
    allArticles.forEach(a => articleCards.appendChild(renderArticleCard(a)));
  }

  // Update title
  if (searchQuery) {
    listTitle.textContent = `Results for "${searchQuery}"`;
  } else if (activeCategory) {
    listTitle.textContent = `Category: ${activeCategory}`;
  } else if (activeTag) {
    listTitle.textContent = `Tag: ${activeTag}`;
  } else {
    listTitle.textContent = 'All Articles';
  }
}

/* ===== Sidebar ===== */
async function loadSidebar() {
  const [cats, tags] = await Promise.all([
    apiFetch('/api/categories'),
    apiFetch('/api/tags'),
  ]);

  // Categories
  categoryList.innerHTML = '';
  const allBtn = document.createElement('li');
  allBtn.innerHTML = `<button class="${!activeCategory ? 'active' : ''}">All Articles</button>`;
  allBtn.querySelector('button').addEventListener('click', () => { activeCategory = null; refreshList(); });
  categoryList.appendChild(allBtn);

  cats.forEach(cat => {
    const li = document.createElement('li');
    const btn = document.createElement('button');
    btn.textContent = cat;
    if (activeCategory === cat) btn.classList.add('active');
    btn.addEventListener('click', () => setCategoryFilter(cat));
    li.appendChild(btn);
    categoryList.appendChild(li);
  });

  // Tags
  tagList.innerHTML = '';
  tags.forEach(tag => tagList.appendChild(renderTagPill(tag, tag === activeTag)));

  // Datalist for editor
  categoryDatalist.innerHTML = '';
  cats.forEach(cat => {
    const opt = document.createElement('option');
    opt.value = cat;
    categoryDatalist.appendChild(opt);
  });
}

function setCategoryFilter(cat) {
  activeCategory = activeCategory === cat ? null : cat;
  activeTag = null;
  searchQuery = '';
  searchInput.value = '';
  refreshList();
}

function toggleTagFilter(tag) {
  activeTag = activeTag === tag ? null : tag;
  activeCategory = null;
  searchQuery = '';
  searchInput.value = '';
  refreshList();
}

async function refreshList() {
  await Promise.all([loadArticleList(), loadSidebar()]);
}

/* ===== Article detail ===== */
async function openArticle(slug) {
  const article = await apiFetch(`/api/articles/${slug}`);
  currentSlug = slug;
  articleContent.innerHTML = `
    <div style="margin-bottom:16px;">
      <span class="tag-pill" style="cursor:default;background:#dbeafe;color:var(--primary);">📁 ${escapeHtml(article.category)}</span>
      ${article.tags.map(t => `<span class="tag-pill" style="cursor:pointer;" onclick="toggleTagFilter('${escapeHtml(t)}')">${escapeHtml(t)}</span>`).join('')}
    </div>
    ${article.html}
  `;
  showDetailView();
}

/* ===== Editor ===== */
function openNewArticle() {
  editingSlug = null;
  document.getElementById('editor-title').textContent = 'New Article';
  document.getElementById('field-slug').disabled = false;
  document.getElementById('article-form').reset();
  document.getElementById('field-category').value = '';
  showEditorView();
}

async function openEditArticle() {
  if (!currentSlug) return;
  const article = await apiFetch(`/api/articles/${currentSlug}`);
  editingSlug = currentSlug;
  document.getElementById('editor-title').textContent = 'Edit Article';
  document.getElementById('field-title').value = article.title;
  document.getElementById('field-slug').value = article.slug;
  document.getElementById('field-slug').disabled = true;
  document.getElementById('field-category').value = article.category;
  document.getElementById('field-tags').value = article.tags.join(', ');
  document.getElementById('field-body').value = article.body;
  showEditorView();
}

async function handleFormSubmit(e) {
  e.preventDefault();
  const title    = document.getElementById('field-title').value.trim();
  const slug     = document.getElementById('field-slug').value.trim();
  const category = document.getElementById('field-category').value.trim() || 'General';
  const tagsRaw  = document.getElementById('field-tags').value.trim();
  const body     = document.getElementById('field-body').value;
  const tags     = tagsRaw ? tagsRaw.split(',').map(t => t.trim()).filter(Boolean) : [];

  try {
    if (editingSlug) {
      await apiFetch(`/api/articles/${editingSlug}`, {
        method: 'PUT',
        body: JSON.stringify({ title, category, tags, body }),
      });
      currentSlug = editingSlug;
    } else {
      await apiFetch('/api/articles', {
        method: 'POST',
        body: JSON.stringify({ slug, title, category, tags, body }),
      });
      currentSlug = slug;
    }
    await refreshList();
    openArticle(currentSlug);
  } catch (err) {
    alert('Error saving article: ' + err.message);
  }
}

/* ===== Delete ===== */
function promptDelete() {
  if (!currentSlug) return;
  const article = allArticles.find(a => a.slug === currentSlug);
  document.getElementById('delete-article-title').textContent = article ? article.title : currentSlug;
  document.getElementById('delete-modal').classList.remove('hidden');
}

async function confirmDelete() {
  document.getElementById('delete-modal').classList.add('hidden');
  await apiFetch(`/api/articles/${currentSlug}`, { method: 'DELETE' });
  showListView();
  await refreshList();
}

/* ===== Search ===== */
let searchTimer;
searchInput.addEventListener('input', () => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    searchQuery = searchInput.value.trim();
    activeCategory = null;
    activeTag = null;
    if (!articleListView.classList.contains('hidden') === false) {
      showListView();
    }
    refreshList();
  }, 250);
});

/* ===== Event listeners ===== */
document.getElementById('new-article-btn').addEventListener('click', openNewArticle);
document.getElementById('back-btn').addEventListener('click', () => { showListView(); });
document.getElementById('edit-btn').addEventListener('click', openEditArticle);
document.getElementById('delete-btn').addEventListener('click', promptDelete);
document.getElementById('article-form').addEventListener('submit', handleFormSubmit);
document.getElementById('cancel-btn').addEventListener('click', () => {
  if (currentSlug) {
    openArticle(currentSlug);
  } else {
    showListView();
  }
});
document.getElementById('cancel-delete-btn').addEventListener('click', () => {
  document.getElementById('delete-modal').classList.add('hidden');
});
document.getElementById('confirm-delete-btn').addEventListener('click', confirmDelete);

// Auto-generate slug from title when creating
document.getElementById('field-title').addEventListener('input', () => {
  if (editingSlug) return;
  const slugField = document.getElementById('field-slug');
  if (!slugField.dataset.manual) {
    slugField.value = document.getElementById('field-title').value
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }
});

document.getElementById('field-slug').addEventListener('input', function () {
  this.dataset.manual = this.value ? '1' : '';
});

/* ===== Init ===== */
(async () => {
  await refreshList();
})();

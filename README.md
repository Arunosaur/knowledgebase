# 📚 Knowledgebase

A simple, file-based knowledgebase application built with Node.js and Express. Articles are stored as Markdown files and served through a clean, responsive web UI.

![Knowledgebase home](https://github.com/user-attachments/assets/ac16c511-28c7-4577-8dee-678be10f5ade)

## Features

- **Browse** articles organized by category and tag
- **Search** full-text across titles, content, and tags
- **Create / Edit / Delete** articles via an in-browser Markdown editor
- **REST API** for programmatic access
- **No database** required — articles are plain Markdown files with YAML front-matter

## Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/) v18 or later

### Install & Run

```bash
npm install
npm start
```

Then open <http://localhost:3000> in your browser.

### Development (auto-restart on file changes)

```bash
npm run dev
```

## Project Structure

```
knowledgebase/
├── server.js          # Express server & REST API
├── articles/          # Markdown article files
│   ├── getting-started.md
│   ├── markdown-cheatsheet.md
│   └── api-reference.md
├── public/            # Static frontend
│   ├── index.html
│   ├── css/style.css
│   └── js/app.js
├── tests/             # Node.js built-in test suite
└── package.json
```

## Article Format

Articles are Markdown files with optional YAML front-matter:

```markdown
---
title: My Article Title
category: General
tags: tag1, tag2, tag3
---

# Article content goes here

Write in standard Markdown...
```

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/articles` | List articles (supports `?q=`, `?tag=`, `?category=`) |
| `GET` | `/api/articles/:slug` | Get a single article (includes rendered HTML) |
| `POST` | `/api/articles` | Create a new article |
| `PUT` | `/api/articles/:slug` | Update an existing article |
| `DELETE` | `/api/articles/:slug` | Delete an article |
| `GET` | `/api/tags` | List all unique tags |
| `GET` | `/api/categories` | List all unique categories |

See the **REST API Reference** article in the app for full details.

## Running Tests

```bash
npm test
```

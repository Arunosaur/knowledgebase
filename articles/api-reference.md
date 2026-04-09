---
title: REST API Reference
category: Documentation
tags: api, rest, reference
---

# REST API Reference

The knowledgebase exposes a RESTful API for managing articles programmatically.

## Base URL

```
http://localhost:3000/api
```

## Endpoints

### Articles

#### List Articles

```
GET /api/articles
```

**Query Parameters:**

| Parameter | Type   | Description                              |
|-----------|--------|------------------------------------------|
| `q`       | string | Full-text search query                   |
| `tag`     | string | Filter by tag                            |
| `category`| string | Filter by category                       |

**Example Response:**

```json
[
  {
    "slug": "getting-started",
    "title": "Getting Started with the Knowledgebase",
    "tags": ["getting-started", "guide"],
    "category": "Documentation"
  }
]
```

#### Get Article

```
GET /api/articles/:slug
```

Returns the full article including rendered HTML.

**Example Response:**

```json
{
  "slug": "getting-started",
  "title": "Getting Started with the Knowledgebase",
  "tags": ["getting-started", "guide"],
  "category": "Documentation",
  "body": "# Getting Started...",
  "html": "<h1>Getting Started...</h1>"
}
```

#### Create Article

```
POST /api/articles
```

**Request Body:**

```json
{
  "slug": "my-article",
  "title": "My Article Title",
  "category": "General",
  "tags": ["tag1", "tag2"],
  "body": "# Article content in Markdown"
}
```

#### Update Article

```
PUT /api/articles/:slug
```

Accepts the same fields as Create (all optional except the slug in the URL).

#### Delete Article

```
DELETE /api/articles/:slug
```

### Tags

#### List All Tags

```
GET /api/tags
```

Returns a sorted array of all unique tags.

### Categories

#### List All Categories

```
GET /api/categories
```

Returns a sorted array of all unique categories.

## Error Responses

| Status | Meaning                              |
|--------|--------------------------------------|
| 400    | Bad request (validation error)       |
| 404    | Article not found                    |
| 409    | Conflict (article already exists)    |

Error responses include a JSON body:

```json
{
  "error": "Description of the error"
}
```

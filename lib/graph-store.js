/**
 * graph-store.js — PostgreSQL + pgvector knowledge graph
 * Manages schema objects, columns, source code, dependencies, and embeddings
 */

const { Pool } = require('pg');

let pool = null;
let embeddingCache = new Map(); // key: first 200 chars of text, value: embedding array
let connectionHealthCache = { timestamp: 0, isConnected: false, cacheTtlMs: 10000 };
let defaultOllamaUrl = process.env.OLLAMA_URL || 'http://localhost:11434';

/**
 * Initialize PostgreSQL connection pool
 */
function initPool(postgresUrl) {
  if (pool) return pool;
  pool = new Pool({
    connectionString: postgresUrl,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });
  pool.on('error', (err) => {
    console.error('[GRAPH] Pool error:', err.message);
  });
  return pool;
}

function setConfig(options = {}) {
  if (options.ollamaUrl) defaultOllamaUrl = String(options.ollamaUrl);
  if (options.postgresUrl && !pool) initPool(String(options.postgresUrl));
}

/**
 * Check if PostgreSQL is connected (with 10s cache)
 */
async function isConnected(postgresUrl) {
  const now = Date.now();
  if (now - connectionHealthCache.timestamp < connectionHealthCache.cacheTtlMs) {
    return connectionHealthCache.isConnected;
  }

  try {
    if (!pool && postgresUrl) {
      initPool(postgresUrl);
    }
    if (!pool) return false;

    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();

    connectionHealthCache = { timestamp: now, isConnected: true, cacheTtlMs: 10000 };
    return true;
  } catch (err) {
    connectionHealthCache = { timestamp: now, isConnected: false, cacheTtlMs: 10000 };
    return false;
  }
}

/**
 * Get connection info for health endpoint
 */
async function getConnectivityInfo(postgresUrl) {
  const connected = await isConnected(postgresUrl);
  if (!connected) {
    return { postgres: false, graphObjects: 0, graphReady: false };
  }

  try {
    if (!pool) initPool(postgresUrl);
    const result = await pool.query('SELECT COUNT(*) as cnt FROM schema_objects');
    const objectCount = parseInt(result.rows[0]?.cnt || 0, 10);
    return {
      postgres: true,
      graphObjects: objectCount,
      graphReady: objectCount > 0,
    };
  } catch (err) {
    return { postgres: false, graphObjects: 0, graphReady: false };
  }
}

/**
 * Initialize schema (create tables and indexes)
 */
async function initSchema(postgresUrl) {
  if (!postgresUrl) {
    console.log('[GRAPH] PostgreSQL disabled (postgresUrl not set)');
    return;
  }

  if (!pool) initPool(postgresUrl);

  try {
    console.log('[GRAPH] Initializing schema...');

    // Enable pgvector extension
    await pool.query('CREATE EXTENSION IF NOT EXISTS vector');

    // schema_objects table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_objects (
        id SERIAL PRIMARY KEY,
        group_id TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        object_name TEXT NOT NULL,
        object_type TEXT NOT NULL,
        status TEXT,
        last_ddl_time TIMESTAMPTZ,
        embedding vector(768),
        synced_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(group_id, schema_name, object_name, object_type)
      )
    `);

    // schema_columns table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_columns (
        id SERIAL PRIMARY KEY,
        object_id INTEGER REFERENCES schema_objects(id) ON DELETE CASCADE,
        column_name TEXT NOT NULL,
        data_type TEXT,
        nullable BOOLEAN,
        column_id INTEGER,
        data_length INTEGER,
        embedding vector(768),
        UNIQUE(object_id, column_name)
      )
    `);

    // schema_source table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_source (
        id SERIAL PRIMARY KEY,
        object_id INTEGER REFERENCES schema_objects(id) ON DELETE CASCADE,
        source_text TEXT,
        line_count INTEGER,
        embedding vector(768),
        UNIQUE(object_id)
      )
    `);

    // schema_dependencies table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS schema_dependencies (
        id SERIAL PRIMARY KEY,
        from_object_id INTEGER REFERENCES schema_objects(id) ON DELETE CASCADE,
        to_object_id INTEGER REFERENCES schema_objects(id) ON DELETE CASCADE,
        dependency_type TEXT DEFAULT 'REFERENCE',
        UNIQUE(from_object_id, to_object_id)
      )
    `);

    // doc_chunks table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS doc_chunks (
        id SERIAL PRIMARY KEY,
        file_id TEXT NOT NULL,
        file_name TEXT,
        title TEXT,
        group_id TEXT,
        site TEXT,
        source TEXT,
        chunk_index INTEGER,
        chunk_text TEXT,
        embedding vector(768),
        last_modified TIMESTAMPTZ,
        synced_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(file_id, chunk_index)
      )
    `);

    // knowledge_entries table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS knowledge_entries (
        id TEXT PRIMARY KEY,
        question TEXT,
        answer TEXT,
        tags TEXT[],
        quality INTEGER DEFAULT 1,
        approved BOOLEAN DEFAULT false,
        captured_by TEXT,
        jira_issues TEXT[],
        embedding vector(768),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // semantic_intents table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS semantic_intents (
        id TEXT PRIMARY KEY,
        intent TEXT,
        keywords TEXT[],
        object_id INTEGER REFERENCES schema_objects(id),
        sql_template TEXT,
        confidence FLOAT DEFAULT 0.5,
        confirmed BOOLEAN DEFAULT false,
        confirmed_by TEXT,
        usage_count INTEGER DEFAULT 0,
        embedding vector(768),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used TIMESTAMPTZ
      )
    `);

    // Create indexes (with try/catch for early tables that might not have enough rows)
    try {
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_so_group
          ON schema_objects(group_id, schema_name)
      `);
      console.log('[GRAPH] ✓ idx_so_group created');
    } catch (err) {
      console.warn('[GRAPH] idx_so_group creation warning:', err.message.slice(0, 100));
    }

    try {
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_so_embedding
          ON schema_objects USING ivfflat (embedding vector_cosine_ops)
          WITH (lists = 100)
      `);
      console.log('[GRAPH] ✓ idx_so_embedding created');
    } catch (err) {
      console.warn('[GRAPH] ivfflat index on schema_objects not yet possible (need >100 rows)');
    }

    try {
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_doc_embedding
          ON doc_chunks USING ivfflat (embedding vector_cosine_ops)
          WITH (lists = 100)
      `);
      console.log('[GRAPH] ✓ idx_doc_embedding created');
    } catch (err) {
      console.warn('[GRAPH] ivfflat index on doc_chunks warning:', err.message.slice(0, 100));
    }

    try {
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_ke_embedding
          ON knowledge_entries USING ivfflat (embedding vector_cosine_ops)
          WITH (lists = 100)
      `);
      console.log('[GRAPH] ✓ idx_ke_embedding created');
    } catch (err) {
      console.warn('[GRAPH] ivfflat index on knowledge_entries warning:', err.message.slice(0, 100));
    }

    try {
      await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_si_embedding
          ON semantic_intents USING ivfflat (embedding vector_cosine_ops)
          WITH (lists = 100)
      `);
      console.log('[GRAPH] ✓ idx_si_embedding created');
    } catch (err) {
      console.warn('[GRAPH] ivfflat index on semantic_intents warning:', err.message.slice(0, 100));
    }

    console.log('[GRAPH] Schema initialized successfully');
  } catch (err) {
    console.error('[GRAPH] Schema init error:', err.message);
    throw err;
  }
}

/**
 * Generate embedding via Ollama
 */
async function generateEmbedding(text, ollamaUrl = defaultOllamaUrl) {
  if (!text || !ollamaUrl) return null;

  const cacheKey = text.slice(0, 200);
  if (embeddingCache.has(cacheKey)) {
    return embeddingCache.get(cacheKey);
  }

  try {
    const response = await fetch(`${ollamaUrl}/api/embeddings`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'nomic-embed-text', prompt: text }),
    });

    if (!response.ok) {
      console.warn(`[GRAPH] Ollama embeddings error: ${response.status}`);
      return null;
    }

    const data = await response.json();
    const embedding = data.embedding;

    if (Array.isArray(embedding)) {
      embeddingCache.set(cacheKey, embedding);
      return embedding;
    }

    return null;
  } catch (err) {
    console.warn('[GRAPH] Embedding generation failed:', err.message.slice(0, 100));
    return null;
  }
}

/**
 * Upsert schema object
 */
async function upsertObject(groupId, schemaName, objectName, objectType, status, lastDdlTime, embedding) {
  if (!pool) return null;

  try {
    const result = await pool.query(
      `INSERT INTO schema_objects (group_id, schema_name, object_name, object_type, status, last_ddl_time, embedding, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (group_id, schema_name, object_name, object_type)
       DO UPDATE SET status = $5, last_ddl_time = $6, embedding = $7, synced_at = NOW()
       RETURNING id`,
      [groupId, schemaName, objectName, objectType, status, lastDdlTime, embedding ? `[${embedding.join(',')}]` : null]
    );

    return result.rows[0]?.id;
  } catch (err) {
    console.error('[GRAPH] upsertObject error:', err.message.slice(0, 100));
    return null;
  }
}

/**
 * Upsert columns for a table
 */
async function upsertColumns(objectId, columns) {
  if (!pool || !objectId) return;

  try {
    for (const col of columns) {
      await pool.query(
        `INSERT INTO schema_columns (object_id, column_name, data_type, nullable, column_id, data_length, embedding)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (object_id, column_name)
         DO UPDATE SET data_type = $3, nullable = $4, column_id = $5, data_length = $6, embedding = $7`,
        [
          objectId,
          col.column_name,
          col.data_type,
          col.nullable,
          col.column_id,
          col.data_length,
          col.embedding ? `[${col.embedding.join(',')}]` : null,
        ]
      );
    }
  } catch (err) {
    console.error('[GRAPH] upsertColumns error:', err.message.slice(0, 100));
  }
}

/**
 * Upsert source code for object
 */
async function upsertSource(objectId, sourceText, embedding) {
  if (!pool || !objectId) return;

  try {
    const lineCount = sourceText ? sourceText.split('\n').length : 0;
    await pool.query(
      `INSERT INTO schema_source (object_id, source_text, line_count, embedding)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (object_id) DO UPDATE
       SET source_text = $2, line_count = $3, embedding = $4`,
      [objectId, sourceText, lineCount, embedding ? `[${embedding.join(',')}]` : null]
    );
  } catch (err) {
    console.error('[GRAPH] upsertSource error:', err.message.slice(0, 100));
  }
}

/**
 * Upsert dependency relationship
 */
async function upsertDependency(fromObjectId, toObjectId, dependencyType = 'REFERENCE') {
  if (!pool || !fromObjectId || !toObjectId) return;

  try {
    await pool.query(
      `INSERT INTO schema_dependencies (from_object_id, to_object_id, dependency_type)
       VALUES ($1, $2, $3)
       ON CONFLICT (from_object_id, to_object_id) DO NOTHING`,
      [fromObjectId, toObjectId, dependencyType]
    );
  } catch (err) {
    console.error('[GRAPH] upsertDependency error:', err.message.slice(0, 100));
  }
}

async function getObjectId(groupId, schemaName, objectName, objectType) {
  if (!pool) return null;
  try {
    const result = await pool.query(
      `SELECT id
       FROM schema_objects
       WHERE group_id = $1 AND schema_name = $2 AND object_name = $3 AND object_type = $4
       LIMIT 1`,
      [groupId, schemaName, objectName, objectType]
    );
    return result.rows[0]?.id || null;
  } catch (_err) {
    return null;
  }
}

/**
 * Upsert document chunk
 */
async function upsertDocChunk(fileId, fileName, title, groupId, site, source, chunkIndex, chunkText, embedding) {
  if (!pool) return;

  try {
    await pool.query(
      `INSERT INTO doc_chunks (file_id, file_name, title, group_id, site, source, chunk_index, chunk_text, embedding, last_modified, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
       ON CONFLICT (file_id, chunk_index)
       DO UPDATE SET chunk_text = $8, embedding = $9, synced_at = NOW()`,
      [fileId, fileName, title, groupId, site, source, chunkIndex, chunkText, embedding ? `[${embedding.join(',')}]` : null]
    );
  } catch (err) {
    console.error('[GRAPH] upsertDocChunk error:', err.message.slice(0, 100));
  }
}

/**
 * Upsert knowledge entry
 */
async function upsertKnowledgeEntry(id, question, answer, tags, quality, approved, capturedBy, jiraIssues, embedding) {
  if (!pool) return;

  try {
    await pool.query(
      `INSERT INTO knowledge_entries (id, question, answer, tags, quality, approved, captured_by, jira_issues, embedding)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id)
       DO UPDATE SET question = $2, answer = $3, tags = $4, quality = $5, approved = $6, embedding = $9`,
      [
        id,
        question,
        answer,
        tags && Array.isArray(tags) ? tags : [],
        quality || 1,
        approved || false,
        capturedBy || null,
        jiraIssues && Array.isArray(jiraIssues) ? jiraIssues : [],
        embedding ? `[${embedding.join(',')}]` : null,
      ]
    );
  } catch (err) {
    console.error('[GRAPH] upsertKnowledgeEntry error:', err.message.slice(0, 100));
  }
}

/**
 * Upsert semantic intent
 */
async function upsertSemanticIntent(id, intent, keywords, objectId, sqlTemplate, confidence, confirmed, confirmedBy, embedding) {
  if (!pool) return;

  try {
    await pool.query(
      `INSERT INTO semantic_intents (id, intent, keywords, object_id, sql_template, confidence, confirmed, confirmed_by, embedding)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (id)
       DO UPDATE SET intent = $2, keywords = $3, confidence = $6, confirmed = $7, confirmed_by = $8, embedding = $9`,
      [
        id,
        intent,
        keywords && Array.isArray(keywords) ? keywords : [],
        objectId || null,
        sqlTemplate || null,
        confidence || 0.5,
        confirmed || false,
        confirmedBy || null,
        embedding ? `[${embedding.join(',')}]` : null,
      ]
    );
  } catch (err) {
    console.error('[GRAPH] upsertSemanticIntent error:', err.message.slice(0, 100));
  }
}

/**
 * Find tables by semantic query
 */
async function findTableByQuery(queryText, groupId, schemaName, limit = 10, ollamaUrl) {
  if (!pool || !queryText) return [];

  try {
    const embedding = await generateEmbedding(queryText, ollamaUrl);

    if (embedding) {
      // Semantic search
      const result = await pool.query(
        `SELECT id, object_name, schema_name, object_type, status,
                1 - (embedding <=> $1::vector) AS similarity
         FROM schema_objects
         WHERE object_type = 'TABLE'
         AND group_id = $2
         AND ($3::text IS NULL OR schema_name = $3)
         ORDER BY embedding <=> $1::vector
         LIMIT $4`,
        [`[${embedding.join(',')}]`, groupId, schemaName || null, limit]
      );

      return result.rows.map((row) => ({
        id: row.id,
        object_name: row.object_name,
        schema_name: row.schema_name,
        object_type: row.object_type,
        status: row.status,
        similarity: parseFloat(row.similarity).toFixed(2),
      }));
    } else {
      // Fallback to ILIKE search
      const result = await pool.query(
        `SELECT id, object_name, schema_name, object_type, status, 0.5 as similarity
         FROM schema_objects
         WHERE object_type = 'TABLE'
         AND group_id = $1
         AND ($2::text IS NULL OR schema_name = $2)
         AND object_name ILIKE $3
         LIMIT $4`,
        [groupId, schemaName || null, `%${queryText}%`, limit]
      );

      return result.rows.map((row) => ({
        id: row.id,
        object_name: row.object_name,
        schema_name: row.schema_name,
        object_type: row.object_type,
        status: row.status,
        similarity: '0.50',
      }));
    }
  } catch (err) {
    console.error('[GRAPH] findTableByQuery error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Find columns by semantic query
 */
async function findColumnsByQuery(queryText, objectId, limit = 10, ollamaUrl) {
  if (!pool || !queryText || !objectId) return [];

  try {
    const embedding = await generateEmbedding(queryText, ollamaUrl);

    if (embedding) {
      // Semantic search
      const result = await pool.query(
        `SELECT column_name, data_type, nullable, column_id,
                1 - (embedding <=> $1::vector) AS similarity
         FROM schema_columns
         WHERE object_id = $2
         ORDER BY embedding <=> $1::vector
         LIMIT $3`,
        [`[${embedding.join(',')}]`, objectId, limit]
      );

      return result.rows;
    } else {
      // Fallback to ILIKE
      const result = await pool.query(
        `SELECT column_name, data_type, nullable, column_id, 0.5 as similarity
         FROM schema_columns
         WHERE object_id = $1
         AND column_name ILIKE $2
         LIMIT $3`,
        [objectId, `%${queryText}%`, limit]
      );

      return result.rows;
    }
  } catch (err) {
    console.error('[GRAPH] findColumnsByQuery error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Semantic document search
 */
async function semanticDocSearch(queryText, groupId,limit = 10, ollamaUrl) {
  if (!pool || !queryText) return [];

  try {
    const embedding = await generateEmbedding(queryText, ollamaUrl);

    if (embedding) {
      const result = await pool.query(
        `SELECT chunk_text, file_name, title, group_id,
                1 - (embedding <=> $1::vector) AS similarity
         FROM doc_chunks
         WHERE ($2::text IS NULL OR group_id = $2)
         ORDER BY embedding <=> $1::vector
         LIMIT $3`,
        [`[${embedding.join(',')}]`, groupId || null, limit]
      );

      return result.rows.map((row) => ({
        chunk_text: row.chunk_text,
        file_name: row.file_name,
        title: row.title,
        group_id: row.group_id,
        similarity: parseFloat(row.similarity).toFixed(2),
      }));
    }

    return [];
  } catch (err) {
    console.error('[GRAPH] semanticDocSearch error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Semantic knowledge search
 */
async function semanticKnowledgeSearch(queryText, limit = 10, ollamaUrl) {
  if (!pool || !queryText) return [];

  try {
    const embedding = await generateEmbedding(queryText, ollamaUrl);

    if (embedding) {
      const result = await pool.query(
        `SELECT id, question, answer, tags, quality,
                1 - (embedding <=> $1::vector) AS similarity
         FROM knowledge_entries
         WHERE approved = true
         ORDER BY embedding <=> $1::vector
         LIMIT $2`,
        [`[${embedding.join(',')}]`, limit]
      );

      return result.rows.map((row) => ({
        id: row.id,
        question: row.question,
        answer: row.answer,
        tags: row.tags,
        quality: row.quality,
        similarity: parseFloat(row.similarity).toFixed(2),
      }));
    }

    return [];
  } catch (err) {
    console.error('[GRAPH] semanticKnowledgeSearch error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Semantic intent search
 */
async function semanticIntentSearch(queryText, limit = 10, ollamaUrl) {
  if (!pool || !queryText) return [];

  try {
    const embedding = await generateEmbedding(queryText, ollamaUrl);

    if (embedding) {
      const result = await pool.query(
        `SELECT id, intent, keywords, sql_template, confidence, confirmed,
                1 - (embedding <=> $1::vector) AS similarity
         FROM semantic_intents
         WHERE confirmed = true
         ORDER BY embedding <=> $1::vector
         LIMIT $2`,
        [`[${embedding.join(',')}]`, limit]
      );

      return result.rows.map((row) => ({
        id: row.id,
        intent: row.intent,
        keywords: row.keywords,
        sql_template: row.sql_template,
        confidence: row.confidence,
        confirmed: row.confirmed,
        similarity: parseFloat(row.similarity).toFixed(2),
      }));
    }

    return [];
  } catch (err) {
    console.error('[GRAPH] semanticIntentSearch error:', err.message.slice(0, 100));
    return [];
  }
}

module.exports = {
  setConfig,
  initPool,
  isConnected,
  getConnectivityInfo,
  initSchema,
  generateEmbedding,
  upsertObject,
  upsertColumns,
  upsertSource,
  upsertDependency,
  getObjectId,
  upsertDocChunk,
  upsertKnowledgeEntry,
  upsertSemanticIntent,
  findTableByQuery,
  findColumnsByQuery,
  semanticDocSearch,
  semanticKnowledgeSearch,
  semanticIntentSearch,
};

/**
 * graph-store.js — PostgreSQL + pgvector knowledge graph
 * Manages schema objects, columns, source code, dependencies, and embeddings
 * Embedding model: mxbai-embed-large (1024 dimensions)
 */

const { Pool } = require('pg');

let pool = null;
let agePool = null;
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
  // Reset search_path on every new connection.
  pool.on('connect', (client) => {
    client.query('SET search_path = public').catch(() => {});
  });
  pool.on('error', (err) => {
    console.error('[GRAPH] Pool error:', err.message);
  });
  return pool;
}

function initAgePool(postgresUrl) {
  if (agePool) return agePool;
  agePool = new Pool({
    connectionString: postgresUrl,
    max: 2,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });
  // Always set AGE context on AGE-only pool connections.
  agePool.on('connect', (client) => {
    client.query("LOAD 'age'")
      .then(() => client.query("SET search_path = ag_catalog, \"$user\", public"))
      .catch(() => {});
  });
  agePool.on('error', (err) => {
    console.error('[GRAPH] AGE pool error:', err.message);
  });
  return agePool;
}

function setConfig(options = {}) {
  if (options.ollamaUrl) defaultOllamaUrl = String(options.ollamaUrl);
  if (options.postgresUrl && !pool) initPool(String(options.postgresUrl));
  if (options.postgresUrl && !agePool) initAgePool(String(options.postgresUrl));
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
 * Checks postgres, graph objects, and AGE graph
 */
async function getConnectivityInfo(postgresUrl) {
  const connected = await isConnected(postgresUrl);
  if (!connected) {
    return { postgres: false, graphObjects: 0, graphReady: false, age: false, ageGraph: null, ageVertices: 0 };
  }

  try {
    if (!pool) initPool(postgresUrl);
    if (!agePool) initAgePool(postgresUrl);

    // Check schema_objects count
    const result = await pool.query('SELECT COUNT(*) as cnt FROM schema_objects');
    const objectCount = parseInt(result.rows[0]?.cnt || 0, 10);

    // Check AGE - must LOAD 'age' on the connection before querying ag_catalog
    let ageInfo = { age: false, ageGraph: null, ageVertices: 0 };
    try {
      const ageClient = await agePool.connect();
      try {
        const ageResult = await ageClient.query(
          "SELECT name FROM ag_catalog.ag_graph WHERE name = 'wms_dependencies'"
        );
        if (ageResult.rows.length > 0) {
          ageInfo = {
            age: true,
            ageGraph: ageResult.rows[0].name,
            ageVertices: 0,
          };
        }
      } finally {
        ageClient.release();
      }
    } catch (ageErr) {
      // AGE not available - not a critical failure
    }

    return {
      postgres: true,
      graphObjects: objectCount,
      graphReady: objectCount > 0,
      ...ageInfo,
    };
  } catch (err) {
    return { postgres: false, graphObjects: 0, graphReady: false, age: false, ageGraph: null, ageVertices: 0 };
  }
}

/**
 * Initialize schema (create tables and indexes)
 * Uses vector(1024) for mxbai-embed-large
 */
async function initSchema(postgresUrl) {
  if (!postgresUrl) {
    console.log('[GRAPH] PostgreSQL disabled (postgresUrl not set)');
    return;
  }

  if (!pool) initPool(postgresUrl);
  if (!agePool) initAgePool(postgresUrl);

  try {
    console.log('[GRAPH] Initializing schema...');

    // Enable pgvector extension
    await pool.query('CREATE EXTENSION IF NOT EXISTS vector');

    // Enable AGE extension and create graph/labels using AGE-only pool
    try {
      await pool.query('CREATE EXTENSION IF NOT EXISTS age');
      const ageClient = await agePool.connect();
      try {
        await ageClient.query('CREATE EXTENSION IF NOT EXISTS age');
        console.log('[GRAPH] ✓ AGE extension enabled');

        const existing = await ageClient.query(
          "SELECT * FROM ag_catalog.ag_graph WHERE name = 'wms_dependencies'"
        );
        if (existing.rows.length === 0) {
          await ageClient.query("SELECT * FROM ag_catalog.create_graph('wms_dependencies')");
          console.log('[GRAPH] ✓ AGE graph wms_dependencies created');
        } else {
          console.log('[GRAPH] ✓ AGE graph wms_dependencies already exists');
        }

        try {
          await ageClient.query("SELECT * FROM ag_catalog.create_vlabel('wms_dependencies', 'OracleObject')");
          console.log('[GRAPH] ✓ AGE vertex label OracleObject created');
        } catch (err) {
          if (!err.message.includes('already exists')) {
            console.warn('[GRAPH] AGE vertex label creation warning:', err.message.slice(0, 100));
          }
        }

        try {
          await ageClient.query("SELECT * FROM ag_catalog.create_elabel('wms_dependencies', 'DEPENDS_ON')");
          console.log('[GRAPH] ✓ AGE edge label DEPENDS_ON created');
        } catch (err) {
          if (!err.message.includes('already exists')) {
            console.warn('[GRAPH] AGE edge label DEPENDS_ON warning:', err.message.slice(0, 100));
          }
        }

        try {
          await ageClient.query("SELECT * FROM ag_catalog.create_elabel('wms_dependencies', 'USED_BY')");
          console.log('[GRAPH] ✓ AGE edge label USED_BY created');
        } catch (err) {
          if (!err.message.includes('already exists')) {
            console.warn('[GRAPH] AGE edge label USED_BY warning:', err.message.slice(0, 100));
          }
        }
      } finally {
        ageClient.release();
      }
    } catch (err) {
      console.warn('[GRAPH] AGE not available:', err.message.slice(0, 100));
    }

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
        embedding vector(1024),
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
        embedding vector(1024),
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
        embedding vector(1024),
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
        embedding vector(1024),
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
        embedding vector(1024),
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
        embedding vector(1024),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used TIMESTAMPTZ
      )
    `);

    // Create indexes (with try/catch — ivfflat needs >100 rows)
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
 * Generate embedding via Ollama (mxbai-embed-large, 1024 dims)
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
      body: JSON.stringify({ model: 'mxbai-embed-large', prompt: text }),
    });

    if (!response.ok) {
      console.warn(`[GRAPH] Ollama embeddings error: ${response.status}`);
      return null;
    }

    const data = await response.json();
    const embedding = data.embedding;

    if (Array.isArray(embedding)) {
      embeddingCache.set(cacheKey, embedding);
      // Evict oldest entry when cache exceeds 5000 entries
      if (embeddingCache.size > 5000) {
        const firstKey = embeddingCache.keys().next().value;
        if (firstKey) embeddingCache.delete(firstKey);
      }
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
 * Find tables by semantic query.
 * Enriches query to match scan embedding format: "TABLE SCHEMA.OBJECTNAME"
 * so that natural language queries like "shipment" match correctly.
 */
async function findTableByQuery(queryText, groupId, schemaName, limit = 10, ollamaUrl) {
  if (!pool || !queryText) return [];

  try {
    // Enrich query to match how objects were embedded during scan
    const enrichedQuery = `TABLE ${schemaName ? schemaName + '.' : ''}${queryText.toUpperCase()}`;
    const embedding = await generateEmbedding(enrichedQuery, ollamaUrl);

    if (embedding) {
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
      // Fallback to ILIKE keyword search
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
 * Find columns by semantic query.
 * Enriches query to match scan embedding format: "TABLENAME.COLUMNNAME DATATYPE"
 */
async function findColumnsByQuery(queryText, objectId, limit = 10, ollamaUrl) {
  if (!pool || !queryText || !objectId) return [];

  try {
    // Get table name for enrichment
    let tableName = '';
    try {
      const tbl = await pool.query(
        'SELECT object_name FROM schema_objects WHERE id = $1 LIMIT 1',
        [objectId]
      );
      tableName = tbl.rows[0]?.object_name || '';
    } catch (_e) { /* ignore — enrichment is best-effort */ }

    // Enrich to match scan format: "TABLENAME.COLUMNNAME DATATYPE"
    const enrichedQuery = tableName
      ? `${tableName}.${queryText.toUpperCase()}`
      : queryText.toUpperCase();

    const embedding = await generateEmbedding(enrichedQuery, ollamaUrl);

    if (embedding) {
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
async function semanticDocSearch(queryText, groupId, limit = 10, ollamaUrl) {
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

// ─────────────────────────────────────────────────────────────────
// ADD THESE TWO FUNCTIONS to graph-store.js
// Place them just before the module.exports block
// Also add them to module.exports: searchDocChunksByTitle, searchDocChunksByText
// ─────────────────────────────────────────────────────────────────

/**
 * Search doc chunks by title containing a code (e.g. "EX01", "SDN-215")
 * Used for extension code lookups — deterministic, no embedding needed.
 * Returns chunks ordered by chunk_index so the answer reads naturally.
 */
async function searchDocChunksByTitle(code, groupId, limit = 10) {
  if (!pool || !code) return [];
  console.log(`[searchDocChunksByTitle] code="${code}" groupId="${groupId}" limit=${limit} pool=${!!pool}`);

  try {
    const result = await pool.query(
      `SELECT chunk_text, file_name, title, group_id,
              1.0 AS similarity
       FROM doc_chunks
       WHERE title ILIKE $1
       AND ($2::text IS NULL OR group_id = $2)
       ORDER BY title, chunk_index
       LIMIT $3`,
      [`%${code}%`, groupId || null, limit]
    );
    console.log(`[searchDocChunksByTitle] rows=${result.rows.length}`);

    return result.rows.map((row) => ({
      chunk_text: row.chunk_text,
      file_name: row.file_name,
      title: row.title,
      group_id: row.group_id,
      similarity: '1.00',
    }));
  } catch (err) {
    console.error('[GRAPH] searchDocChunksByTitle error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Search doc chunks where chunk_text contains the code string.
 * Fallback when title search finds nothing.
 */
async function searchDocChunksByText(code, groupId, limit = 10) {
  if (!pool || !code) return [];

  try {
    const result = await pool.query(
      `SELECT chunk_text, file_name, title, group_id,
              0.95 AS similarity
       FROM doc_chunks
       WHERE chunk_text ILIKE $1
       AND ($2::text IS NULL OR group_id = $2)
       ORDER BY title, chunk_index
       LIMIT $3`,
      [`%${code}%`, groupId || null, limit]
    );

    return result.rows.map((row) => ({
      chunk_text: row.chunk_text,
      file_name: row.file_name,
      title: row.title,
      group_id: row.group_id,
      similarity: '0.95',
    }));
  } catch (err) {
    console.error('[GRAPH] searchDocChunksByText error:', err.message.slice(0, 100));
    return [];
  }
}

/**
 * Upsert vertex into AGE graph
 * id format: "{groupId}::{schemaName}::{objectName}::{objectType}"
 */
async function upsertAgeVertex(groupId, schemaName, objectName, objectType, status = null) {
  if (!agePool) return null;

  try {
    const id = `${groupId}::${schemaName}::${objectName}::${objectType}`;
    const client = await agePool.connect();
    try {
      const params = JSON.stringify({
        id,
        groupId,
        schema: schemaName,
        name: objectName,
        type: objectType,
        status: status || '',
      }).replace(/'/g, "''");
      const result = await client.query(`
        SELECT * FROM cypher('wms_dependencies', $$
          MERGE (o:OracleObject {
            id: $id,
            groupId: $groupId,
            schema: $schema,
            name: $name,
            type: $type,
            status: $status
          })
          RETURN o
        $$, '${params}') AS (vertex agtype);
      `);

      return result.rows[0]?.vertex;
    } finally {
      client.release();
    }
  } catch (err) {
    console.warn('[GRAPH] upsertAgeVertex warning:', err.message.slice(0, 100));
    return null;
  }
}

/**
 * Upsert edge into AGE graph
 */
async function upsertAgeEdge(fromId, toId, dependencyType = 'REFERENCE') {
  if (!agePool) return null;

  try {
    const client = await agePool.connect();
    try {
      const params = JSON.stringify({
        fromId,
        toId,
        dependencyType,
      }).replace(/'/g, "''");
      const result = await client.query(`
        SELECT * FROM cypher('wms_dependencies', $$
          MATCH (a:OracleObject {id: $fromId}),
                (b:OracleObject {id: $toId})
          MERGE (a)-[r:DEPENDS_ON {type: $dependencyType}]->(b)
          RETURN r
        $$, '${params}') AS (edge agtype);
      `);

      return result.rows[0]?.edge;
    } finally {
      client.release();
    }
  } catch (err) {
    console.warn('[GRAPH] upsertAgeEdge warning:', err.message.slice(0, 100));
    return null;
  }
}

/**
 * Traverse AGE graph for impact analysis
 * direction: 'outbound' (what this depends on) or 'inbound' (what uses this)
 */
async function traverseImpact(objectId, direction = 'both', maxDepth = 3) {
  if (!agePool) return { nodes: [], edges: [], queryMs: 0 };

  const startTime = Date.now();

  try {
    const client = await agePool.connect();
    try {
      const params = JSON.stringify({ id: objectId, depth: maxDepth }).replace(/'/g, "''");
      const result = await client.query(`
        SELECT * FROM cypher('wms_dependencies', $$
          MATCH path = (start:OracleObject {id: $id})-[:DEPENDS_ON*1..$depth]-(related)
          RETURN related, length(path) as depth
          LIMIT 200
        $$, '${params}') AS (related agtype, depth agtype);
      `);

      const queryMs = Date.now() - startTime;
      const nodes = result.rows.map((row) => ({
        id: row.related?.properties?.id || objectId,
        name: row.related?.properties?.name || '?',
        schema: row.related?.properties?.schema || '?',
        type: row.related?.properties?.type || '?',
        depth: row.depth || 0,
      }));

      return {
        nodes,
        edges: [],
        queryMs,
        truncated: nodes.length >= 200,
      };
    } finally {
      client.release();
    }
  } catch (err) {
    console.warn('[GRAPH] traverseImpact warning:', err.message.slice(0, 100));
    return { nodes: [], edges: [], queryMs: Date.now() - startTime };
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
  searchDocChunksByTitle,
  searchDocChunksByText,
  upsertAgeVertex,
  upsertAgeEdge,
  traverseImpact,
};
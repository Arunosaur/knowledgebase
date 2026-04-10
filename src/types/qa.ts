export interface DocHit {
  chunk_text?: string;
  text?: string;
  file_name?: string;
  fileName?: string;
  title?: string;
  group_id?: string;
  similarity?: string;
  fileId?: string;
  score?: number;
}

export interface KnowledgeHit {
  id?: string;
  question?: string;
  answer?: string;
  approved?: boolean;
}

export interface JiraHit {
  key?: string;
  summary?: string;
  status?: string;
  type?: string;
  description?: string;
}

export interface DbHit {
  summary?: string;
  details?: string;
}

export interface QAMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  docHits: DocHit[];
  knowledgeHits: KnowledgeHit[];
  jiraHits: JiraHit[];
  dbHits: DbHit[];
  timestamp: Date;
}

export interface DocHit {
  fileId?: string;
  fileName?: string;
  text?: string;
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

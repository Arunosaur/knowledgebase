import type { BootstrapConfig, Group, HealthStatus } from '../types/config';
import type { ImpactGraphResponse, SchemaObject } from '../types/graph';
import type { KnowledgeHit, DocHit, JiraHit } from '../types/qa';
import type { SemanticIntent } from '../types/semantic';

const isDev = Boolean(import.meta.env.DEV);
const devBase = '/api';
const prodBase = typeof window !== 'undefined' ? window.location.origin : '';

function buildUrl(path: string): string {
  const normalized = path.startsWith('/') ? path : `/${path}`;
  if (isDev) return `${devBase}${normalized}`;
  return `${prodBase}${normalized}`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(buildUrl(path), {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers || {}),
    },
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(String((payload as { error?: string }).error || `Request failed: ${response.status}`));
  }
  return payload as T;
}

export const bridgeApi = {
  request,
  health: () => request<HealthStatus>('/health'),
  config: () => request<BootstrapConfig>('/config'),
  groups: () => request<Group[]>('/groups'),
  ollamaModels: () => request<{ models: Array<string | { name: string }> }>('/ollama/models'),
  ollamaChat: (body: { model: string; messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>; stream: false }) =>
    request<{ response?: string; choices?: Array<{ message?: { content?: string } }> }>('/ollama/chat', {
      method: 'POST',
      body: JSON.stringify(body),
    }),

  dbSchemas: (group: string) => request<Array<{ SCHEMA_NAME: string }>>(`/db/schemas?group=${encodeURIComponent(group)}`),
  dbObjects: (group: string, schema: string) =>
    request<SchemaObject[]>(`/db/objects?group=${encodeURIComponent(group)}&schema=${encodeURIComponent(schema)}`),
  dbImpact: (params: { group: string; schema: string; name: string; type?: string; depth?: number }) => {
    const q = new URLSearchParams({
      group: params.group,
      schema: params.schema,
      name: params.name,
    });
    if (params.type) q.set('type', params.type);
    if (params.depth) q.set('depth', String(params.depth));
    return request<ImpactGraphResponse>(`/db/impact?${q.toString()}`);
  },
  dbImpactGraph: (params: { group: string; schema: string; name: string; type?: string; depth?: number; direction?: string }) => {
    const q = new URLSearchParams({
      group: params.group,
      schema: params.schema,
      name: params.name,
    });
    if (params.type) q.set('type', params.type);
    if (params.depth) q.set('depth', String(params.depth));
    if (params.direction) q.set('direction', params.direction);
    return request<ImpactGraphResponse>(`/db/impact-graph?${q.toString()}`);
  },

  docsList: () => request<{ files?: DocHit[] } | DocHit[]>('/docs/list'),
  docsSearch: (q: string, limit = 10, group?: string) =>
    request<DocHit[]>(`/docs/search?q=${encodeURIComponent(q)}&limit=${limit}${group ? `&group=${encodeURIComponent(group)}` : ''}`),

  knowledgeSearch: (q: string, limit = 5) =>
    request<KnowledgeHit[]>(`/knowledge/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  knowledgeList: (limit = 200) => request<{ entries: KnowledgeHit[] }>(`/knowledge/list?limit=${limit}`),

  jiraSearch: (q: string, maxResults = 10) =>
    request<{ issues: JiraHit[] }>(`/jira/search?q=${encodeURIComponent(q)}&maxResults=${maxResults}`),

  semanticList: () => request<{ intents: SemanticIntent[] }>('/semantic/list'),
  semanticStatus: () => request<{ running?: boolean; paused?: boolean }>('/semantic/scan-status'),
};

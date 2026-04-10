import { useMemo, useState } from 'react';
import type { QAMessage } from '../types/qa';

const MODEL_LIMITS: Record<string, number> = {
  'qwen2.5:14b': 32768,
  'gemma4:26b': 131072,
  llama3: 8192,
};

function randomId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function useConversation(modelName: string, systemPrompt: string) {
  const [messages, setMessages] = useState<QAMessage[]>([]);
  const [trimNote, setTrimNote] = useState('');

  const modelLimit = MODEL_LIMITS[modelName] || 8192;

  const tokenEstimate = useMemo(() => {
    const historyChars = messages.reduce((acc, msg) => acc + msg.content.length, 0);
    return Math.ceil((systemPrompt.length + historyChars) / 4);
  }, [messages, systemPrompt]);

  const usagePercent = Math.min(100, Math.round((tokenEstimate / modelLimit) * 100));

  const appendUser = (content: string) => {
    setMessages((prev) => [
      ...prev,
      {
        id: randomId(),
        role: 'user',
        content,
        docHits: [],
        knowledgeHits: [],
        jiraHits: [],
        dbHits: [],
        timestamp: new Date(),
      },
    ]);
  };

  const appendAssistant = (payload: Omit<QAMessage, 'id' | 'role' | 'timestamp'>) => {
    setMessages((prev) => [
      ...prev,
      {
        id: randomId(),
        role: 'assistant',
        timestamp: new Date(),
        ...payload,
      },
    ]);
  };

  const getHistoryForModel = () => {
    const turns = messages.slice(-16);
    if (usagePercent < 95) return turns;
    setTrimNote('Oldest messages trimmed to fit context');
    return turns.slice(-8);
  };

  const clearConversation = () => {
    setMessages([]);
    setTrimNote('');
  };

  const summarizeAndReset = (summary: string) => {
    setMessages([
      {
        id: randomId(),
        role: 'user',
        content: `Continuing from previous session: ${summary}`,
        docHits: [],
        knowledgeHits: [],
        jiraHits: [],
        dbHits: [],
        timestamp: new Date(),
      },
    ]);
    setTrimNote('');
  };

  return {
    messages,
    tokenEstimate,
    modelLimit,
    usagePercent,
    trimNote,
    appendUser,
    appendAssistant,
    clearConversation,
    summarizeAndReset,
    getHistoryForModel,
  };
}

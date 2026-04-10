import { useMemo, useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { Group } from '../../types/config';
import { ChatThread } from './ChatThread';
import { ContextBar } from './ContextBar';
import { LearningHint } from './LearningHint';
import { QuestionInput } from './QuestionInput';
import { SampleQuestions } from './SampleQuestions';
import { useConversation } from '../../hooks/useConversation';

type Props = {
  activeGroup: Group | null;
  defaultModel: string;
};

export function AskMode({ activeGroup, defaultModel }: Props) {
  const [loading, setLoading] = useState(false);
  const [learningHint, setLearningHint] = useState('');
  const [showContextWarning, setShowContextWarning] = useState(true);

  const systemPrompt = useMemo(
    () =>
      [
        'You are McLane WMS·IQ assistant.',
        'Use provided context only. Do not hallucinate ticket keys or schema objects.',
        activeGroup ? `Primary group: ${activeGroup.id}` : 'Primary group: auto',
      ].join('\n'),
    [activeGroup]
  );

  const convo = useConversation(defaultModel, systemPrompt);

  const ask = async (question: string) => {
    setLoading(true);
    try {
      // Enrich follow-up searches when the new question is vague.
      const lastUserMsg = convo.messages.filter((m) => m.role === 'user').slice(-1)[0]?.content || '';
      const isVague = question.length < 40 && !question.match(/\bEX\d+\b|\bSDN\b|\bDOCK\b/i);
      const searchQuery = isVague && lastUserMsg ? `${lastUserMsg} ${question}` : question;

      convo.appendUser(question);

      const [docsHits, knowledgeHits, jiraPayload] = await Promise.all([
        bridgeApi.docsSearch(searchQuery, 3, activeGroup?.id).catch(() => []),
        bridgeApi.knowledgeSearch(searchQuery, 5).catch(() => []),
        bridgeApi.jiraSearch(searchQuery, 5).catch(() => ({ issues: [] })),
      ]);

      if (!docsHits.length && !knowledgeHits.length && !jiraPayload.issues?.length) {
        convo.appendAssistant({
          content: "I don't have enough context to answer that follow-up. Could you rephrase with more detail?",
          docHits: [],
          knowledgeHits: [],
          jiraHits: [],
          dbHits: [],
        });
        return;
      }

      const docBlock = docsHits.length
        ? `\n=====================================\nRELEVANT DOCUMENTS (📄)\n=====================================\n${docsHits
            .slice(0, 3)
            .map((h: any, index: number) => `[${index + 1}] ${h.title || h.file_name || h.fileName || ''}\n${(h.chunk_text || h.text || '').slice(0, 800)}`)
            .join('\n\n')}`
        : '';

      const knowledgeBlock = knowledgeHits.length
        ? `\n=====================================\nRELEVANT KNOWLEDGE (🧠)\n=====================================\n${knowledgeHits
            .map((hit: any, index: number) => `[${index + 1}] ${hit.question}\n${hit.answer}`)
            .join('\n\n')}`
        : '';

      const jiraIssues = jiraPayload.issues || [];
      const jiraBlock = jiraIssues.length
        ? `\n=====================================\nRELEVANT JIRA ISSUES (🎫)\n=====================================\n${jiraIssues
            .slice(0, 5)
            .map((issue: any) => `${issue.key} [${issue.type || 'Issue'}/${issue.status || 'Unknown'}] ${issue.summary}`)
            .join('\n')}`
        : '';

      const noContextGuard = !docsHits.length && !knowledgeHits.length && !jiraIssues.length
        ? '\n\nYou have no relevant context for this question. Do NOT invent an answer. Say: "I don\'t have enough information to answer this question."'
        : '';

      const fullSystemPrompt = [
        systemPrompt,
        learningHint ? `\nLearning hint: ${learningHint}` : '',
        docBlock,
        knowledgeBlock,
        jiraBlock,
        noContextGuard,
        '\n\nCRITICAL: Answer based ONLY on the context provided above. Never invent table names, procedure names, or ticket numbers.',
      ].join('');

      const history = convo.getHistoryForModel().map((m) => ({ role: m.role, content: m.content }));
      const messages = [
        { role: 'system' as const, content: fullSystemPrompt },
        ...history.slice(-8),
        { role: 'user' as const, content: question },
      ];

      const ai = await bridgeApi.ollamaChat({
        model: defaultModel,
        messages,
        stream: false,
      });

      const content =
        ai.response ||
        ai.choices?.[0]?.message?.content ||
        'I do not have enough information to answer right now.';

      convo.appendAssistant({
        content,
        docHits: docsHits,
        knowledgeHits,
        jiraHits: jiraIssues,
        dbHits: [],
      });
    } finally {
      setLoading(false);
    }
  };

  const summarizeAndStartNew = async () => {
    const transcript = convo.messages
      .map((m) => `${m.role.toUpperCase()}: ${m.content}`)
      .join('\n\n')
      .slice(0, 20000);

    const summaryResp = await bridgeApi.ollamaChat({
      model: defaultModel,
      messages: [
        {
          role: 'system',
          content:
            'Summarize this conversation into a compact session brief covering key WMS topics discussed, schemas/DCs mentioned, conclusions reached, and open questions. Max 200 words.',
        },
        {
          role: 'user',
          content: transcript,
        },
      ],
      stream: false,
    });

    const summary = summaryResp.response || summaryResp.choices?.[0]?.message?.content || '';
    if (summary && navigator.clipboard) {
      navigator.clipboard.writeText(summary).catch(() => undefined);
    }
    convo.summarizeAndReset(summary);
    setShowContextWarning(false);
  };

  return (
    <section className="ask-mode">
      <h2>Ask a Question</h2>
      <SampleQuestions onPick={(q) => ask(q).catch(() => undefined)} />
      <ChatThread messages={convo.messages} />
      <LearningHint value={learningHint} onChange={setLearningHint} />
      <QuestionInput onAsk={ask} onNewConversation={convo.clearConversation} loading={loading} />
      <ContextBar
        usagePercent={convo.usagePercent}
        tokenEstimate={convo.tokenEstimate}
        modelLimit={convo.modelLimit}
        nearLimit={convo.usagePercent >= 95}
        showWarning={showContextWarning && convo.usagePercent >= 90}
        onKeepGoing={() => setShowContextWarning(false)}
        onSummarize={() => summarizeAndStartNew().catch(() => undefined)}
      />
      {convo.trimNote && <div className="trim-note">{convo.trimNote}</div>}
    </section>
  );
}

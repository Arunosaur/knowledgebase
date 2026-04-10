import type { QAMessage } from '../../types/qa';

type Props = { message: QAMessage };

export function MessageBubble({ message }: Props) {
  return (
    <div className={`msg msg-${message.role}`}>
      <div className="msg-role">{message.role === 'user' ? 'You' : 'WMS·IQ'}</div>
      <div className="msg-content">{message.content}</div>
      {message.role === 'assistant' && (
        <div className="msg-pills">
          <span>📄 {message.docHits.length}</span>
          <span>🧠 {message.knowledgeHits.length}</span>
          <span>🎫 {message.jiraHits.length}</span>
          <span>🗄️ {message.dbHits.length}</span>
        </div>
      )}
    </div>
  );
}

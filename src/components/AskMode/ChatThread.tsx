import type { QAMessage } from '../../types/qa';
import { MessageBubble } from './MessageBubble';

type Props = { messages: QAMessage[] };

export function ChatThread({ messages }: Props) {
  if (messages.length === 0) {
    return <div className="empty-thread">Ask a WMS question to start the conversation.</div>;
  }

  return (
    <div className="thread">
      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
      ))}
    </div>
  );
}

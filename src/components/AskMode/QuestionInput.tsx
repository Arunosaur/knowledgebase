import { useState } from 'react';

type Props = {
  onAsk: (question: string) => Promise<void>;
  onNewConversation: () => void;
  loading: boolean;
};

export function QuestionInput({ onAsk, onNewConversation, loading }: Props) {
  const [value, setValue] = useState('');

  const submit = async () => {
    const trimmed = value.trim();
    if (!trimmed) return;
    setValue('');
    await onAsk(trimmed);
  };

  return (
    <div className="input-row">
      <textarea
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="Ask about WMS behavior, incidents, dependencies, or docs..."
        rows={3}
      />
      <div className="input-actions">
        <button onClick={submit} disabled={loading}>{loading ? 'Asking...' : 'Ask'}</button>
        <button className="secondary" onClick={onNewConversation}>New Conversation</button>
      </div>
    </div>
  );
}

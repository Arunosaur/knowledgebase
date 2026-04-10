type Props = {
  usagePercent: number;
  tokenEstimate: number;
  modelLimit: number;
  nearLimit: boolean;
  showWarning: boolean;
  onKeepGoing: () => void;
  onSummarize: () => void;
};

function usageColor(percent: number): string {
  if (percent >= 90) return '#f85149';
  if (percent >= 70) return '#d29922';
  return '#3fb950';
}

export function ContextBar({
  usagePercent,
  tokenEstimate,
  modelLimit,
  nearLimit,
  showWarning,
  onKeepGoing,
  onSummarize,
}: Props) {
  const width = `${Math.max(2, usagePercent)}%`;
  const barColor = usageColor(usagePercent);

  return (
    <div className="context-wrap">
      <div className="context-line">
        Context: {usagePercent}% used {tokenEstimate.toLocaleString()} / {modelLimit.toLocaleString()} tokens
      </div>
      <div className="context-track">
        <div className="context-fill" style={{ width, backgroundColor: barColor }} />
      </div>
      {nearLimit && <div className="context-note">Oldest messages trimmed to fit context.</div>}
      {showWarning && (
        <div className="context-warning">
          <div>You're near the context limit ({usagePercent}%).</div>
          <div className="context-actions">
            <button onClick={onSummarize}>Summarize &amp; start new chat</button>
            <button onClick={onKeepGoing}>Keep going until limit</button>
          </div>
        </div>
      )}
    </div>
  );
}

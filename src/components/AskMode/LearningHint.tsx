type Props = {
  value: string;
  onChange: (v: string) => void;
};

export function LearningHint({ value, onChange }: Props) {
  return (
    <div className="hint-wrap">
      <label htmlFor="learning-hint">Optional learning hint</label>
      <input
        id="learning-hint"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder="Paste SQL or implementation hint to steer this answer"
      />
    </div>
  );
}

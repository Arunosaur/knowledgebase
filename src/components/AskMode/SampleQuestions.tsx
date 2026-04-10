type Props = {
  onPick: (q: string) => void;
};

const SAMPLE = [
  'Which DCs have the most open waves today?',
  'Show top unresolved WMSHUB incidents this week.',
  'What does DOCK_PK depend on in MANH_CODE?',
  'Summarize EX01 configuration behavior from docs.',
];

export function SampleQuestions({ onPick }: Props) {
  return (
    <div className="sample-wrap">
      {SAMPLE.map((question) => (
        <button className="sample-chip" key={question} onClick={() => onPick(question)}>
          {question}
        </button>
      ))}
    </div>
  );
}

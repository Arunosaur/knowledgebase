import type { HealthStatus } from '../../types/config';

type Props = { health: HealthStatus | null };

function Dot({ ok }: { ok: boolean }) {
  return <span className={`dot ${ok ? 'ok' : 'bad'}`} />;
}

export function HealthBadge({ health }: Props) {
  if (!health) return <div className="health-badge">Health: loading...</div>;
  return (
    <div className="health-badge">
      <span><Dot ok={!!health.postgres} /> Postgres</span>
      <span><Dot ok={!!health.ollama} /> Ollama</span>
      <span><Dot ok={!!health.atlassian} /> Atlassian</span>
    </div>
  );
}

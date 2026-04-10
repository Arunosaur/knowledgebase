import { useEffect, useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { KnowledgeHit } from '../../types/qa';

export function KnowledgeTab() {
  const [entries, setEntries] = useState<KnowledgeHit[]>([]);

  useEffect(() => {
    bridgeApi.knowledgeList(200)
      .then((r) => setEntries(r.entries || []))
      .catch(() => setEntries([]));
  }, []);

  return (
    <section className="panel-card">
      <h3>Knowledge</h3>
      <div>{entries.length} entries</div>
      <div className="table-list">
        {entries.slice(0, 50).map((entry, idx) => (
          <div key={`${entry.id || idx}`}>{entry.question || entry.id || 'entry'}</div>
        ))}
      </div>
    </section>
  );
}

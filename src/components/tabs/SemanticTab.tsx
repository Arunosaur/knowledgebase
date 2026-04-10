import { useEffect, useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { SemanticIntent } from '../../types/semantic';

export function SemanticTab() {
  const [intents, setIntents] = useState<SemanticIntent[]>([]);

  useEffect(() => {
    bridgeApi.semanticList()
      .then((r) => setIntents(r.intents || []))
      .catch(() => setIntents([]));
  }, []);

  return (
    <section className="panel-card">
      <h3>Semantic Intents</h3>
      <div>{intents.length} intents</div>
      <div className="table-list">
        {intents.slice(0, 50).map((intent) => (
          <div key={intent.id}>{intent.intent} ({Math.round((intent.confidence || 0) * 100)}%)</div>
        ))}
      </div>
    </section>
  );
}

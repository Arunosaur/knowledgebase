import { useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { Group } from '../../types/config';
import type { ImpactGraphResponse } from '../../types/graph';

type Props = { activeGroup: Group | null };

export function ImpactAnalysis({ activeGroup }: Props) {
  const [name, setName] = useState('DOCK_PK');
  const [schema, setSchema] = useState('MANH_CODE');
  const [depth, setDepth] = useState(3);
  const [result, setResult] = useState<ImpactGraphResponse | null>(null);

  const run = async () => {
    if (!activeGroup) return;
    const r = await bridgeApi.dbImpactGraph({ group: activeGroup.id, schema, name, depth, direction: 'both' });
    setResult(r);
  };

  return (
    <section className="panel-card">
      <h3>Impact Analysis</h3>
      <div className="inline-form">
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Object name" />
        <input value={schema} onChange={(e) => setSchema(e.target.value)} placeholder="Schema" />
        <input type="number" value={depth} min={1} max={5} onChange={(e) => setDepth(Number(e.target.value))} />
        <button onClick={() => run().catch(() => undefined)}>Run</button>
      </div>
      {result && <pre>{JSON.stringify(result, null, 2)}</pre>}
    </section>
  );
}

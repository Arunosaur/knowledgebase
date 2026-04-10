import { useEffect, useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { Group } from '../../types/config';
import type { SchemaObject } from '../../types/graph';

type Props = { activeGroup: Group | null };

export function SchemaExplorer({ activeGroup }: Props) {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState('');
  const [objects, setObjects] = useState<SchemaObject[]>([]);

  useEffect(() => {
    if (!activeGroup) return;
    bridgeApi.dbSchemas(activeGroup.id)
      .then((rows) => {
        const names = rows.map((x) => x.SCHEMA_NAME);
        setSchemas(names);
        setSelectedSchema(names[0] || '');
      })
      .catch(() => {
        setSchemas([]);
        setSelectedSchema('');
      });
  }, [activeGroup]);

  useEffect(() => {
    if (!activeGroup || !selectedSchema) return;
    bridgeApi.dbObjects(activeGroup.id, selectedSchema)
      .then(setObjects)
      .catch(() => setObjects([]));
  }, [activeGroup, selectedSchema]);

  return (
    <section className="panel-card">
      <h3>Schema Explorer</h3>
      <select value={selectedSchema} onChange={(e) => setSelectedSchema(e.target.value)}>
        {schemas.map((schema) => <option key={schema}>{schema}</option>)}
      </select>
      <div className="table-list">
        {objects.slice(0, 100).map((obj) => (
          <div key={`${obj.OBJECT_NAME}-${obj.OBJECT_TYPE}`}>{obj.OBJECT_NAME} <span>{obj.OBJECT_TYPE}</span></div>
        ))}
      </div>
    </section>
  );
}

import { useEffect, useState } from 'react';
import { bridgeApi } from '../../api/bridge';
import type { DocHit } from '../../types/qa';

export function DocsTab() {
  const [files, setFiles] = useState<DocHit[]>([]);

  useEffect(() => {
    bridgeApi.docsList()
      .then((payload) => {
        if (Array.isArray(payload)) setFiles(payload as DocHit[]);
        else setFiles((payload.files || []) as DocHit[]);
      })
      .catch(() => setFiles([]));
  }, []);

  return (
    <section className="panel-card">
      <h3>Docs Library</h3>
      <div>{files.length} indexed files</div>
      <div className="table-list">
        {files.slice(0, 50).map((f, idx) => (
          <div key={`${f.fileId || f.fileName || idx}`}>{f.fileName || f.fileId || 'doc'}</div>
        ))}
      </div>
    </section>
  );
}

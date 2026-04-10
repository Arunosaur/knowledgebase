import { useState } from 'react';
import type { Group, HealthStatus } from '../../types/config';
import { GroupCard } from './GroupCard';
import { SchemaExplorer } from './SchemaExplorer';
import { ImpactAnalysis } from './ImpactAnalysis';
import { ErdDiagram } from './ErdDiagram';
import { AskAITab } from '../tabs/AskAITab';
import { DocsTab } from '../tabs/DocsTab';
import { KnowledgeTab } from '../tabs/KnowledgeTab';
import { SemanticTab } from '../tabs/SemanticTab';
import { HomeTab } from '../tabs/HomeTab';

type TabKey = 'home' | 'impact' | 'schema' | 'erd' | 'askai' | 'docs' | 'knowledge' | 'semantic';

type Props = {
  groups: Group[];
  activeGroup: Group | null;
  health: HealthStatus | null;
  setActiveGroupId: (id: string) => void;
  defaultModel: string;
};

export function ExploreMode({ groups, activeGroup, health, setActiveGroupId, defaultModel }: Props) {
  const [tab, setTab] = useState<TabKey>('home');

  if (!activeGroup) {
    return (
      <section>
        <h2>Explore Systems</h2>
        <div className="group-grid">
          {groups.map((group) => (
            <GroupCard key={group.id} group={group} health={health} onSelect={setActiveGroupId} />
          ))}
        </div>
      </section>
    );
  }

  return (
    <section>
      <div className="explore-header">
        <h2>Explore Systems</h2>
        <select value={activeGroup.id} onChange={(e) => setActiveGroupId(e.target.value)}>
          {groups.map((g) => <option key={g.id} value={g.id}>{g.name}</option>)}
        </select>
      </div>
      <div className="tab-row">
        <button onClick={() => setTab('home')}>Home</button>
        <button onClick={() => setTab('impact')}>Impact</button>
        <button onClick={() => setTab('schema')}>Schema</button>
        <button onClick={() => setTab('erd')}>ERD</button>
        <button onClick={() => setTab('askai')}>Ask AI</button>
        <button onClick={() => setTab('docs')}>Docs</button>
        <button onClick={() => setTab('knowledge')}>Knowledge</button>
        <button onClick={() => setTab('semantic')}>Semantic</button>
      </div>

      {tab === 'home' && <HomeTab group={activeGroup} health={health} />}
      {tab === 'impact' && <ImpactAnalysis activeGroup={activeGroup} />}
      {tab === 'schema' && <SchemaExplorer activeGroup={activeGroup} />}
      {tab === 'erd' && <ErdDiagram schema={activeGroup.schemas?.[0] || 'MANH_CODE'} />}
      {tab === 'askai' && <AskAITab group={activeGroup} model={defaultModel} />}
      {tab === 'docs' && <DocsTab />}
      {tab === 'knowledge' && <KnowledgeTab />}
      {tab === 'semantic' && <SemanticTab />}
    </section>
  );
}

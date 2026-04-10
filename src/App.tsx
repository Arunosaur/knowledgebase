import { useState } from 'react';
import { useBridge } from './hooks/useBridge';
import { useGroups } from './hooks/useGroups';
import { useHealth } from './api/useHealth';
import { AskMode } from './components/AskMode/AskMode';
import { ExploreMode } from './components/ExploreMode/ExploreMode';
import { HealthBadge } from './components/shared/HealthBadge';
import { DemoModeBanner } from './components/shared/DemoModeBanner';
import { Settings } from './components/shared/Settings';
import { Toast } from './components/shared/Toast';

type Mode = 'ask' | 'explore';

export default function App() {
  const [mode, setMode] = useState<Mode>('ask');
  const [toast] = useState('');
  const { config } = useBridge();
  const { groups, activeGroup, setActiveGroupId } = useGroups();
  const { health } = useHealth();

  return (
    <div className="app-shell">
      <header className="hero">
        <h1>McLane WMS·IQ</h1>
        <p>Navigate the McLane WMS landscape</p>
        <div className="mode-row">
          <button className={mode === 'ask' ? 'active' : ''} onClick={() => setMode('ask')}>Ask a Question</button>
          <button className={mode === 'explore' ? 'active' : ''} onClick={() => setMode('explore')}>Explore Systems</button>
        </div>
      </header>

      <HealthBadge health={health} />
      <DemoModeBanner enabled={false} />

      {mode === 'ask' ? (
        <AskMode
          activeGroup={activeGroup}
          defaultModel={config?.bridge.defaultModel || health?.model || 'qwen2.5:14b'}
        />
      ) : (
        <ExploreMode
          groups={groups}
          activeGroup={activeGroup}
          health={health}
          setActiveGroupId={setActiveGroupId}
          defaultModel={config?.bridge.defaultModel || health?.model || 'qwen2.5:14b'}
        />
      )}

      <Settings config={config} />
      <Toast message={toast} />
    </div>
  );
}

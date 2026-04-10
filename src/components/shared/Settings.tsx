import type { BootstrapConfig } from '../../types/config';

type Props = {
  config: BootstrapConfig | null;
};

export function Settings({ config }: Props) {
  return (
    <div className="settings-panel">
      <h3>Settings</h3>
      <div>Model: {config?.bridge.defaultModel || 'unknown'}</div>
      <div>Ollama: {config?.bridge.ollamaUrl || 'unknown'}</div>
      <div>Docs max results: {config?.bridge.docsMaxResults ?? 10}</div>
      <div>Q&A context char limit: {config?.bridge.qaContextCharLimit ?? 12000}</div>
    </div>
  );
}

import type { Group, HealthStatus } from '../../types/config';

type Props = {
  group: Group;
  health: HealthStatus | null;
  onSelect: (groupId: string) => void;
};

export function GroupCard({ group, health, onSelect }: Props) {
  return (
    <button className="group-card" onClick={() => onSelect(group.id)}>
      <div className="group-title">{group.icon} {group.name}</div>
      <div className="group-meta">{group.id}</div>
      <div className="group-meta">Env: {group.env}</div>
      <div className="group-meta">Connected user: {(group as { dbUser?: string | null }).dbUser || 'unknown'}</div>
      <div className="group-health">Bridge: {health?.bridge ? 'online' : 'offline'}</div>
    </button>
  );
}

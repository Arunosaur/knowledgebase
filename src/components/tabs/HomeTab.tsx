import type { Group, HealthStatus } from '../../types/config';

type Props = { group: Group | null; health: HealthStatus | null };

export function HomeTab({ group, health }: Props) {
  return (
    <section className="panel-card">
      <h3>WMS Intelligence Overview</h3>
      <div>Active group: {group?.name || 'none'}</div>
      <div>Graph objects: {health?.graphObjects || 0}</div>
      <div>AGE graph: {health?.age ? health.ageGraph : 'unavailable'}</div>
    </section>
  );
}

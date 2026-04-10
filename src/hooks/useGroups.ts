import { useCallback, useEffect, useMemo, useState } from 'react';
import { bridgeApi } from '../api/bridge';
import type { Group } from '../types/config';

export function useGroups() {
  const [groups, setGroups] = useState<Group[]>([]);
  const [activeGroupId, setActiveGroupId] = useState<string>('');

  const load = useCallback(async () => {
    const next = await bridgeApi.groups();
    setGroups(next);
    if (!activeGroupId && next.length > 0) {
      setActiveGroupId(next[0].id);
    }
  }, [activeGroupId]);

  useEffect(() => {
    load().catch(() => undefined);
  }, [load]);

  const activeGroup = useMemo(
    () => groups.find((g) => g.id === activeGroupId) || null,
    [groups, activeGroupId]
  );

  return { groups, activeGroup, activeGroupId, setActiveGroupId, reloadGroups: load };
}

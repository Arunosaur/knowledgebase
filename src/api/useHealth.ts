import { useCallback, useEffect, useState } from 'react';
import { bridgeApi } from './bridge';
import type { HealthStatus } from '../types/config';

export function useHealth(pollMs = 30000) {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setHealth(await bridgeApi.health());
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh().catch(() => undefined);
    const id = window.setInterval(() => {
      refresh().catch(() => undefined);
    }, pollMs);
    return () => window.clearInterval(id);
  }, [pollMs, refresh]);

  return { health, loading, refresh };
}

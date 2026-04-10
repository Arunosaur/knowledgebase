import { useCallback, useEffect, useState } from 'react';
import { bridgeApi } from '../api/bridge';
import type { BootstrapConfig } from '../types/config';

export function useBridge() {
  const [config, setConfig] = useState<BootstrapConfig | null>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setConfig(await bridgeApi.config());
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load().catch(() => undefined);
  }, [load]);

  return { config, loading, reload: load };
}

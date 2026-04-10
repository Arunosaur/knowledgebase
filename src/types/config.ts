export interface Group {
  id: string;
  name: string;
  env: 'prod' | 'uat' | 'test';
  connectionName: string;
  schemas: string[];
  color: string;
  icon: string;
  readOnly: boolean;
  description: string;
  dbUser?: string | null;
}

export interface DistributionCenter {
  code: string;
  dcId: number;
  name: string;
  type: string;
  active: boolean;
  manhattanGroup: string | null;
  dmSchema: string | null;
  mdaSchema: string | null;
  cigwmsGroup: string | null;
  wmshubGroup: string | null;
}

export interface BridgeConfig {
  port: number;
  ollamaUrl: string;
  defaultModel: string;
  postgresEnabled: boolean;
  qaContextCharLimit: number;
  docsMaxResults: number;
  atlassianEnabled: boolean;
}

export interface HealthStatus {
  bridge: boolean;
  ollama: boolean;
  postgres: boolean;
  atlassian: boolean;
  graphReady: boolean;
  graphObjects: number;
  model: string;
  groups: number;
  age?: boolean;
  ageGraph?: string | null;
  ageVertices?: number;
}

export interface BootstrapConfig {
  bridge: {
    port: number;
    semanticWorkerPort: number;
    ollamaUrl: string;
    defaultModel: string;
    docsMaxResults: number;
    qaContextCharLimit: number;
    atlassianEnabled: boolean;
  };
  groups: Group[];
  distributionCenters: DistributionCenter[];
}

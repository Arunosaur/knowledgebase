export interface SchemaObject {
  OBJECT_NAME: string;
  OBJECT_TYPE: string;
  STATUS?: string;
}

export interface ImpactNode {
  id: string;
  name: string;
  schema: string;
  type: string;
  group?: string;
  status?: string;
  depth?: number;
}

export interface ImpactEdge {
  from: string;
  to: string;
  type?: string;
}

export interface ImpactGraphResponse {
  root: {
    name: string;
    type: string;
    schema: string;
    group: string;
  };
  nodes: ImpactNode[];
  edges: ImpactEdge[];
  crossSchemaEdges: ImpactEdge[];
  truncated: boolean;
  queryMs: number;
  graphBacked?: boolean;
  error?: string | null;
}

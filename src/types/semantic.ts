export type ConfidenceLevel = 'high' | 'medium' | 'low';

export interface SemanticIntent {
  id: string;
  intent: string;
  confidence: number;
  confirmed: boolean;
  source?: string;
  sqlTemplate?: string;
  keywords?: string[];
}

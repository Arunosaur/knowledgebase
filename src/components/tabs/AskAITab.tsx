import type { Group } from '../../types/config';
import { AskMode } from '../AskMode/AskMode';

type Props = { group: Group | null; model: string };

export function AskAITab({ group, model }: Props) {
  return <AskMode activeGroup={group} defaultModel={model} />;
}

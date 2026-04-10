type Props = { enabled: boolean };

export function DemoModeBanner({ enabled }: Props) {
  if (!enabled) return null;
  return <div className="demo-banner">Demo mode enabled: write operations are restricted.</div>;
}

type Props = { schema: string };

export function ErdDiagram({ schema }: Props) {
  return (
    <section className="panel-card">
      <h3>ERD Diagram</h3>
      <div className="erd-placeholder">ERD SVG renderer placeholder for {schema}</div>
    </section>
  );
}

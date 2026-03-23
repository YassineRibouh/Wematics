export function Card({ title, actions, children, className = "" }) {
  return (
    <section className={`card ${className}`.trim()}>
      {(title || actions) && (
        <header className="card-header">
          {title ? <h2>{title}</h2> : <span />}
          {actions}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, tone = "neutral" }) {
  return (
    <div className={`stat ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}


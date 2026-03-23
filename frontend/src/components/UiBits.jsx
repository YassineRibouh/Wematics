export function Loading({ text = "Loading..." }) {
  return <div className="loading">{text}</div>;
}

export function ErrorBlock({ error, onRetry }) {
  return (
    <div className="error-block">
      <p>{String(error?.message || error || "Unknown error")}</p>
      {onRetry ? (
        <button type="button" onClick={onRetry}>
          Retry
        </button>
      ) : null}
    </div>
  );
}

export function Empty({ text = "No data" }) {
  return <div className="empty">{text}</div>;
}

export function Notice({ tone = "info", text }) {
  if (!text) return null;
  return <div className={`notice ${tone}`}>{text}</div>;
}

import { useEffect, useMemo, useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading } from "../components/UiBits";
import { VirtualList } from "../components/VirtualList";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

const LOG_LEVELS = ["", "INFO", "WARNING", "ERROR"];
const AUDIT_SOURCES = ["", "remote", "local", "ftp"];

function eventKey(row, idx) {
  return `${row.created_at}-${row.job_id || row.filename || "item"}-${idx}`;
}

function formatFieldLabel(value) {
  const text = String(value || "")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text ? text.replace(/\b\w/g, (char) => char.toUpperCase()) : "-";
}

function formatTimestamp(value) {
  if (!value) return "-";
  return String(value).replace("T", " ").replace(/\.\d+$/, "").replace(/Z$/, " UTC");
}

function hasObjectEntries(value) {
  return Boolean(value && typeof value === "object" && Object.keys(value).length);
}

function formatDateWindow(params = {}) {
  if (params.date) return params.date;
  if (params.date_from || params.date_to) {
    const from = params.date_from || params.date_to;
    const to = params.date_to || params.date_from;
    return from === to ? from : `${from} to ${to}`;
  }
  if (params.rolling_days) return `Last ${params.rolling_days} days`;
  if (params.backfill_months) return `Backfill ${params.backfill_months} months`;
  if (params.mode === "latest_only") return "Latest only";
  return params.mode ? formatFieldLabel(params.mode) : "-";
}

function buildJobFacts(job) {
  if (!job) return [];

  const params = job.params || {};
  const facts = [];
  const scope = [params.camera, params.variable].filter(Boolean).join(" / ");
  const window = formatDateWindow(params);

  if (job.created_at) facts.push({ label: "Created", value: formatTimestamp(job.created_at) });
  if (scope) facts.push({ label: "Scope", value: scope });
  if (window !== "-") facts.push({ label: "Window", value: window });
  if (job.progress?.stage) facts.push({ label: "Stage", value: formatFieldLabel(job.progress.stage) });
  facts.push({ label: "Retries", value: `${job.retry_count}/${job.max_retries}` });

  return facts;
}

export function LogsPage() {
  const [tab, setTab] = useState("logs");
  const [logFilters, setLogFilters] = useState({ q: "", level: "", job_id: "", camera: "", variable: "", date: "", filename: "" });
  const [auditFilters, setAuditFilters] = useState({ q: "", source: "", action: "", job_id: "", camera: "", variable: "", date: "", filename: "" });
  const [selectedLogKey, setSelectedLogKey] = useState("");
  const [selectedAuditKey, setSelectedAuditKey] = useState("");

  const logs = useAsync(() => api.getLogs({ ...logFilters, limit: 400 }), [logFilters], true);
  const audit = useAsync(() => api.getAudit({ ...auditFilters, limit: 500 }), [auditFilters], true);

  const selectedLog = useMemo(
    () => (logs.data?.events || []).find((row, idx) => eventKey(row, idx) === selectedLogKey) || null,
    [logs.data, selectedLogKey]
  );
  const selectedAudit = useMemo(
    () => (audit.data?.events || []).find((row, idx) => eventKey(row, idx) === selectedAuditKey) || null,
    [audit.data, selectedAuditKey]
  );
  const activeSelection = tab === "logs" ? selectedLog : selectedAudit;
  const activeJobId = activeSelection?.job_id || "";
  const job = useAsync(() => (activeJobId ? api.getJob(activeJobId) : Promise.resolve(null)), [activeJobId], !!activeJobId);
  const jobEvents = useAsync(() => (activeJobId ? api.getJobEvents(activeJobId) : Promise.resolve([])), [activeJobId], !!activeJobId);
  const jobSummaryFacts = useMemo(() => buildJobFacts(job.data), [job.data]);

  useEffect(() => {
    if (logs.data?.events?.length && (!selectedLogKey || !logs.data.events.some((row, idx) => eventKey(row, idx) === selectedLogKey))) {
      setSelectedLogKey(eventKey(logs.data.events[0], 0));
    }
  }, [logs.data, selectedLogKey]);

  useEffect(() => {
    if (audit.data?.events?.length && (!selectedAuditKey || !audit.data.events.some((row, idx) => eventKey(row, idx) === selectedAuditKey))) {
      setSelectedAuditKey(eventKey(audit.data.events[0], 0));
    }
  }, [audit.data, selectedAuditKey]);

  const runLogs = () => logs.run().catch(() => {});
  const runAudit = () => audit.run().catch(() => {});

  return (
    <>
      <div className="page-header">
        <div>
          <p className="page-kicker">Traceability</p>
          <h2 className="page-title">Logs & Audit Drilldown</h2>
          <p className="page-subtitle">Filter aggressively, inspect one event at a time, and pivot into the job that produced it.</p>
        </div>
      </div>

      <Card
        title="View"
        actions={
          <div className="controls">
            <button className={tab === "logs" ? "" : "secondary"} onClick={() => setTab("logs")}>
              Logs
            </button>
            <button className={tab === "audit" ? "" : "secondary"} onClick={() => setTab("audit")}>
              File Audit
            </button>
          </div>
        }
      >
        <div className="pill-row">
          <span className="tag">Job events for operational failures and retries</span>
          <span className="tag">File audit for lifecycle and skip reasons</span>
        </div>
      </Card>

      {tab === "logs" ? (
        <Card title="Log Filters" actions={<button onClick={runLogs}>Refresh</button>}>
          <div className="controls controls-grid">
            <label>
              Search text
              <input value={logFilters.q} onChange={(e) => setLogFilters((prev) => ({ ...prev, q: e.target.value }))} placeholder="message or filename" />
            </label>
            <label>
              Level
              <select value={logFilters.level} onChange={(e) => setLogFilters((prev) => ({ ...prev, level: e.target.value }))}>
                {LOG_LEVELS.map((level) => (
                  <option key={level || "all"} value={level}>
                    {level || "all"}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Job ID
              <input value={logFilters.job_id} onChange={(e) => setLogFilters((prev) => ({ ...prev, job_id: e.target.value }))} />
            </label>
            <label>
              Camera
              <input value={logFilters.camera} onChange={(e) => setLogFilters((prev) => ({ ...prev, camera: e.target.value }))} />
            </label>
            <label>
              Variable
              <input value={logFilters.variable} onChange={(e) => setLogFilters((prev) => ({ ...prev, variable: e.target.value }))} />
            </label>
            <label>
              Date
              <input type="date" value={logFilters.date} onChange={(e) => setLogFilters((prev) => ({ ...prev, date: e.target.value }))} />
            </label>
            <label>
              Filename
              <input value={logFilters.filename} onChange={(e) => setLogFilters((prev) => ({ ...prev, filename: e.target.value }))} />
            </label>
          </div>
        </Card>
      ) : (
        <Card title="Audit Filters" actions={<button onClick={runAudit}>Refresh</button>}>
          <div className="controls controls-grid">
            <label>
              Search text
              <input value={auditFilters.q} onChange={(e) => setAuditFilters((prev) => ({ ...prev, q: e.target.value }))} placeholder="filename, action, reason" />
            </label>
            <label>
              Source
              <select value={auditFilters.source} onChange={(e) => setAuditFilters((prev) => ({ ...prev, source: e.target.value }))}>
                {AUDIT_SOURCES.map((item) => (
                  <option key={item || "all"} value={item}>
                    {item || "all"}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Action
              <input value={auditFilters.action} onChange={(e) => setAuditFilters((prev) => ({ ...prev, action: e.target.value }))} placeholder="uploaded_ftp" />
            </label>
            <label>
              Job ID
              <input value={auditFilters.job_id} onChange={(e) => setAuditFilters((prev) => ({ ...prev, job_id: e.target.value }))} />
            </label>
            <label>
              Camera
              <input value={auditFilters.camera} onChange={(e) => setAuditFilters((prev) => ({ ...prev, camera: e.target.value }))} />
            </label>
            <label>
              Variable
              <input value={auditFilters.variable} onChange={(e) => setAuditFilters((prev) => ({ ...prev, variable: e.target.value }))} />
            </label>
            <label>
              Date
              <input type="date" value={auditFilters.date} onChange={(e) => setAuditFilters((prev) => ({ ...prev, date: e.target.value }))} />
            </label>
            <label>
              Filename
              <input value={auditFilters.filename} onChange={(e) => setAuditFilters((prev) => ({ ...prev, filename: e.target.value }))} />
            </label>
          </div>
        </Card>
      )}

      <div className="grid two two-left-wide">
        <Card
          title={tab === "logs" ? "Filtered Logs" : "Filtered Audit Events"}
          actions={<span className="mono">{tab === "logs" ? logs.data?.events?.length || 0 : audit.data?.events?.length || 0} rows</span>}
        >
          {tab === "logs" ? (
            logs.loading && !logs.data ? (
              <Loading text="Loading logs..." />
            ) : logs.error ? (
              <ErrorBlock error={logs.error} onRetry={runLogs} />
            ) : logs.data?.events?.length ? (
              <VirtualList
                items={logs.data.events}
                height={560}
                rowHeight={62}
                renderItem={(row, idx) => (
                  <button
                    key={eventKey(row, idx)}
                    type="button"
                    className={`result-row result-row-three ${selectedLogKey === eventKey(row, idx) ? "selected" : ""}`}
                    onClick={() => setSelectedLogKey(eventKey(row, idx))}
                  >
                    <span className="mono">{row.created_at}</span>
                    <span className={`tag ${row.level === "ERROR" ? "alert" : row.level === "INFO" ? "ok" : ""}`}>{row.level}</span>
                    <span className="mono">{row.job_id ? row.job_id.slice(0, 8) : "-"}</span>
                    <span>{row.message}</span>
                  </button>
                )}
              />
            ) : (
              <Empty text="No logs matched the current filters." />
            )
          ) : audit.loading && !audit.data ? (
            <Loading text="Loading audit events..." />
          ) : audit.error ? (
            <ErrorBlock error={audit.error} onRetry={runAudit} />
          ) : audit.data?.events?.length ? (
            <VirtualList
              items={audit.data.events}
              height={560}
              rowHeight={62}
              renderItem={(row, idx) => (
                <button
                  key={eventKey(row, idx)}
                  type="button"
                  className={`result-row result-row-three ${selectedAuditKey === eventKey(row, idx) ? "selected" : ""}`}
                  onClick={() => setSelectedAuditKey(eventKey(row, idx))}
                >
                  <span className="mono">{row.created_at}</span>
                  <span className={`tag ${row.source === "ftp" ? "ok" : row.source === "remote" ? "alert" : ""}`}>{row.source || "-"}</span>
                  <span>{row.action}</span>
                  <span className="mono result-row-name">{row.filename}</span>
                </button>
              )}
            />
          ) : (
            <Empty text="No audit events matched the current filters." />
          )}
        </Card>

        <Card title="Event Detail">
          {!activeSelection ? (
            <Empty text="Select a log or audit row to inspect details." />
          ) : (
            <div className="detail-stack">
              <div className="detail-card">
                <h3>Selected Event</h3>
                <div className="detail-list">
                  {Object.entries(activeSelection).map(([key, value]) => (
                    <div key={key} className="detail-row detail-row-spacious">
                      <span className="detail-row-label">{formatFieldLabel(key)}</span>
                      <span>{typeof value === "object" && value !== null ? JSON.stringify(value) : String(value ?? "-")}</span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="grid two compact-grid">
                <div className="detail-card">
                  <h3>Job Summary</h3>
                  {activeJobId ? (
                    job.loading && !job.data ? (
                      <Loading text="Loading job..." />
                    ) : job.error ? (
                      <ErrorBlock error={job.error} onRetry={job.run} />
                    ) : job.data ? (
                      <div className="job-summary-panel">
                        <div className="job-summary-head">
                          <div>
                            <p className="job-summary-kicker">Linked Job</p>
                            <div className="job-summary-id mono">{job.data.id}</div>
                          </div>
                          <div className="job-summary-badges">
                            <span className={`tag ${job.data.status === "failed" ? "alert" : job.data.status === "completed" ? "ok" : ""}`}>
                              {formatFieldLabel(job.data.status)}
                            </span>
                            <span className="tag">{formatFieldLabel(job.data.kind)}</span>
                          </div>
                        </div>
                        <div className="job-summary-facts">
                          {jobSummaryFacts.map((item) => (
                            <div key={item.label} className="job-summary-fact">
                              <span>{item.label}</span>
                              <strong>{item.value}</strong>
                            </div>
                          ))}
                        </div>
                        {job.data.error_summary ? (
                          <div className="job-summary-error">
                            <span className="job-summary-error-label">Error</span>
                            <p>{job.data.error_summary}</p>
                          </div>
                        ) : null}
                        {hasObjectEntries(job.data.params) ? (
                          <details className="summary-disclosure">
                            <summary>
                              <span>Request Details</span>
                              <span className="summary-disclosure-hint">Expand</span>
                            </summary>
                            <div className="summary-disclosure-body">
                              <pre className="json-block">{JSON.stringify(job.data.params, null, 2)}</pre>
                            </div>
                          </details>
                        ) : null}
                        {hasObjectEntries(job.data.progress) ? (
                          <details className="summary-disclosure">
                            <summary>
                              <span>Progress Payload</span>
                              <span className="summary-disclosure-hint">Expand</span>
                            </summary>
                            <div className="summary-disclosure-body">
                              <pre className="json-block">{JSON.stringify(job.data.progress, null, 2)}</pre>
                            </div>
                          </details>
                        ) : null}
                      </div>
                    ) : null
                  ) : (
                    <Empty text="No related job for this event." />
                  )}
                </div>

                <div className="detail-card">
                  <h3>Job Event Stream</h3>
                  {activeJobId ? (
                    jobEvents.loading && !jobEvents.data ? (
                      <Loading text="Loading job events..." />
                    ) : jobEvents.error ? (
                      <ErrorBlock error={jobEvents.error} onRetry={jobEvents.run} />
                    ) : jobEvents.data?.length ? (
                      <div className="event-stream-list">
                        {jobEvents.data.slice(-8).reverse().map((event, idx) => (
                          <div key={`${event.created_at}-${idx}`} className="event-stream-row">
                            <div className="event-stream-meta">
                              <span className="mono">{formatTimestamp(event.created_at)}</span>
                              <span className={`tag ${event.level === "ERROR" ? "alert" : event.level === "INFO" ? "ok" : ""}`}>{event.level}</span>
                            </div>
                            <p className="event-stream-message">{event.message}</p>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <Empty text="No events for this job." />
                    )
                  ) : (
                    <Empty text="Select a row with a job ID to load its event stream." />
                  )}
                </div>
              </div>
            </div>
          )}
        </Card>
      </div>
    </>
  );
}

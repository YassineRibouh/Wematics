import { useEffect, useMemo, useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading, Notice } from "../components/UiBits";
import { VirtualList } from "../components/VirtualList";
import { api } from "../lib/api";
import { formatBytes, formatNumber, isImageFilename } from "../lib/format";
import { useAsync } from "../lib/useAsync";

const SOURCE_OPTIONS = ["", "remote", "local", "ftp"];

function resultKey(item) {
  return [item.source, item.camera, item.variable, item.date, item.filename].join("|");
}

function SourceCard({ title, record, kind }) {
  if (!record) {
    return (
      <div className="lineage-source-card lineage-source-card-empty">
        <span className="tag">{title}</span>
        <p>Not present</p>
      </div>
    );
  }

  const base = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api";
  let action = null;
  if (kind === "remote" && isImageFilename(record.filename)) {
    action = `${base}/remote/preview?camera=${encodeURIComponent(record.camera)}&variable=${encodeURIComponent(record.variable)}&date=${encodeURIComponent(
      record.date
    )}&filename=${encodeURIComponent(record.filename)}&timezone=local`;
  }
  if (kind === "local" && isImageFilename(record.filename)) {
    action = `${base}/local/preview?camera=${encodeURIComponent(record.camera)}&variable=${encodeURIComponent(record.variable)}&date=${encodeURIComponent(
      record.date
    )}&filename=${encodeURIComponent(record.filename)}`;
  }
  if (kind === "ftp" && record.ftp_path) {
    action = api.getFtpServerDownloadUrl(record.ftp_path);
  }

  return (
    <div className="lineage-source-card">
      <div className="lineage-source-head">
        <span className="tag ok">{title}</span>
        {action ? (
          <a className="link-button secondary" href={action} target="_blank" rel="noreferrer">
            Open
          </a>
        ) : null}
      </div>
      <p className="mono">Timestamp: {record.parsed_timestamp || "-"}</p>
      <p className="mono">Size: {formatBytes(record.file_size || 0)}</p>
      <p className="mono">Checksum: {record.checksum || "-"}</p>
      <p className="mono">Seen: {record.seen_at || "-"}</p>
      {record.local_path ? <p className="mono">Local: {record.local_path}</p> : null}
      {record.ftp_path ? <p className="mono">FTP: {record.ftp_path}</p> : null}
      <div className="pill-row">
        <span className={`tag ${record.downloaded ? "ok" : ""}`}>downloaded: {String(!!record.downloaded)}</span>
        <span className={`tag ${record.uploaded ? "ok" : ""}`}>uploaded: {String(!!record.uploaded)}</span>
        <span className={`tag ${record.verified ? "ok" : ""}`}>verified: {String(!!record.verified)}</span>
      </div>
    </div>
  );
}

export function SearchPage() {
  const [q, setQ] = useState("");
  const [source, setSource] = useState("");
  const [camera, setCamera] = useState("");
  const [variable, setVariable] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [filters, setFilters] = useState(null);
  const [selectedKey, setSelectedKey] = useState("");

  const search = useAsync(() => (filters ? api.searchFiles({ ...filters, page_size: 250 }) : Promise.resolve(null)), [filters], true);
  const selectedItem = useMemo(
    () => (search.data?.items || []).find((item) => resultKey(item) === selectedKey) || null,
    [search.data, selectedKey]
  );
  const lineage = useAsync(
    () => (selectedItem ? api.getFileLineage(selectedItem) : Promise.resolve(null)),
    [selectedItem],
    !!selectedItem
  );

  useEffect(() => {
    if (!search.data?.items?.length) {
      setSelectedKey("");
      return;
    }
    if (!selectedKey || !search.data.items.some((item) => resultKey(item) === selectedKey)) {
      setSelectedKey(resultKey(search.data.items[0]));
    }
  }, [search.data, selectedKey]);

  const runSearch = () => {
    const next = {
      q: q.trim(),
      source,
      camera: camera.trim(),
      variable: variable.trim(),
      date_from: dateFrom,
      date_to: dateTo,
    };
    setFilters(next);
    setSelectedKey("");
  };

  const sourceCards = lineage.data?.sources || {};

  return (
    <>
      <div className="page-header page-header-search">
        <div>
          <p className="page-kicker">Cross-Source Lookup</p>
          <h2 className="page-title">Global Search & File Lineage</h2>
          <p className="page-subtitle">Find a file once, then inspect where it exists, which job touched it, and what happened over time.</p>
        </div>
        <div className="page-header-actions">
          <button onClick={runSearch}>Search</button>
        </div>
      </div>

      <Card title="Search Filters" className="card-hero">
        <div className="controls controls-grid">
          <label>
            Filename contains
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="rgb.webp or cloud_cover" />
          </label>
          <label>
            Source
            <select value={source} onChange={(e) => setSource(e.target.value)}>
              {SOURCE_OPTIONS.map((item) => (
                <option key={item || "all"} value={item}>
                  {item || "all"}
                </option>
              ))}
            </select>
          </label>
          <label>
            Camera
            <input value={camera} onChange={(e) => setCamera(e.target.value)} placeholder="ROS" />
          </label>
          <label>
            Variable
            <input value={variable} onChange={(e) => setVariable(e.target.value)} placeholder="RGB" />
          </label>
          <label>
            Date from
            <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </label>
          <label>
            Date to
            <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </label>
        </div>
        <div className="pill-row" style={{ marginTop: "0.9rem" }}>
          <span className="tag">Searches across remote, local, and FTP records</span>
          <span className="tag">Returns up to 250 rows</span>
        </div>
      </Card>

      {search.error ? <ErrorBlock error={search.error} onRetry={runSearch} /> : null}

      <div className="grid two two-left-wide">
        <Card
          title="Search Results"
          actions={<span className="mono">{formatNumber(search.data?.total || 0)} match(es)</span>}
        >
          {!search.data && !search.loading ? <Notice text="Set filters and run a search." /> : null}
          {search.loading && !search.data ? (
            <Loading text="Searching file records..." />
          ) : search.data?.items?.length ? (
            <VirtualList
              items={search.data.items}
              height={520}
              rowHeight={54}
              renderItem={(item) => (
                <button
                  key={resultKey(item)}
                  type="button"
                  className={`result-row ${selectedKey === resultKey(item) ? "selected" : ""}`}
                  onClick={() => setSelectedKey(resultKey(item))}
                >
                  <span className={`tag ${item.source === "ftp" ? "ok" : item.source === "local" ? "" : "alert"}`}>{item.source}</span>
                  <span className="mono">{item.date}</span>
                  <span>{item.camera}</span>
                  <span>{item.variable}</span>
                  <span className="mono result-row-name">{item.filename}</span>
                  <span className="mono">{formatBytes(item.file_size || 0)}</span>
                </button>
              )}
            />
          ) : search.data ? (
            <Empty text="No files matched this query." />
          ) : null}
        </Card>

        <Card title="Lineage Detail">
          {!selectedItem ? (
            <Empty text="Select a result to inspect lineage." />
          ) : lineage.loading && !lineage.data ? (
            <Loading text="Loading lineage..." />
          ) : lineage.error ? (
            <ErrorBlock error={lineage.error} onRetry={lineage.run} />
          ) : lineage.data ? (
            <div className="detail-stack">
              <div>
                <p className="mono">{lineage.data.filename}</p>
                <div className="pill-row">
                  <span className={`tag ${lineage.data.presence?.remote ? "ok" : "alert"}`}>remote</span>
                  <span className={`tag ${lineage.data.presence?.local ? "ok" : "alert"}`}>local</span>
                  <span className={`tag ${lineage.data.presence?.ftp ? "ok" : "alert"}`}>ftp</span>
                </div>
              </div>

              <div className="grid three compact-grid">
                <SourceCard title="Remote" record={sourceCards.remote} kind="remote" />
                <SourceCard title="Local" record={sourceCards.local} kind="local" />
                <SourceCard title="FTP" record={sourceCards.ftp} kind="ftp" />
              </div>

              <div className="grid two compact-grid">
                <div className="detail-card">
                  <h3>Related Jobs</h3>
                  {lineage.data.related_jobs?.length ? (
                    <div className="detail-list">
                      {lineage.data.related_jobs.slice(0, 12).map((job) => (
                        <div key={job.id} className="detail-row">
                          <span className="mono">{job.id.slice(0, 8)}</span>
                          <span>{job.kind}</span>
                          <span className={`tag ${job.status === "completed" ? "ok" : job.status === "failed" ? "alert" : ""}`}>
                            {job.status}
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <Empty text="No related jobs recorded." />
                  )}
                </div>

                <div className="detail-card">
                  <h3>Audit Trail</h3>
                  {lineage.data.audit_events?.length ? (
                    <div className="detail-list">
                      {lineage.data.audit_events.slice(0, 12).map((event, index) => (
                        <div key={`${event.created_at}-${index}`} className="detail-row detail-row-wide">
                          <span className="mono">{event.created_at}</span>
                          <span>{event.action}</span>
                          <span>{event.reason || "-"}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <Empty text="No audit events recorded." />
                  )}
                </div>
              </div>

              <div className="detail-card">
                <h3>Job Events</h3>
                {lineage.data.job_events?.length ? (
                  <div className="detail-list">
                    {lineage.data.job_events.slice(0, 15).map((event, index) => (
                      <div key={`${event.created_at}-${index}`} className="detail-row detail-row-wide">
                        <span className="mono">{event.created_at}</span>
                        <span>{event.level}</span>
                        <span>{event.message}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <Empty text="No file-specific job events recorded." />
                )}
              </div>
            </div>
          ) : null}
        </Card>
      </div>
    </>
  );
}




import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Card } from "../components/Card";
import { VirtualList } from "../components/VirtualList";
import { Empty, ErrorBlock, Loading, Notice } from "../components/UiBits";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

const RESUMABLE_KINDS = new Set(["download", "upload", "transfer"]);

function scopeDigest(values) {
  return Object.values(values)
    .map((value) => (value === null || value === undefined || value === "" ? "-" : String(value)))
    .join("|");
}

function titleizeToken(value) {
  const text = String(value || "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : "-";
}

function formatTimestamp(value) {
  if (!value) return "-";
  return String(value).replace("T", " ").replace(/\.\d+$/, "").replace(/Z$/, " UTC");
}

function shortId(value) {
  return value ? String(value).slice(0, 8) : "-";
}

function queueStatusTone(status) {
  if (status === "failed" || status === "cancelled") return "alert";
  if (status === "completed") return "ok";
  return "";
}

function dateWindowForMode(mode, singleDate, dateFrom, dateTo) {
  if (mode === "single_date") return { from: singleDate || "", to: singleDate || "" };
  if (mode === "date_range") return { from: dateFrom || "", to: dateTo || dateFrom || "" };
  return { from: dateFrom || "", to: dateTo || dateFrom || "" };
}

function formatWindowLabel(mode, singleDate, dateWindow, rollingDays, backfillMonths) {
  if (mode === "single_date") return singleDate || "Pick a date";
  if (mode === "date_range") {
    if (dateWindow.from && dateWindow.to) return `${dateWindow.from} to ${dateWindow.to}`;
    return "Pick a range";
  }
  if (mode === "rolling_days") return `Last ${rollingDays || 0} days`;
  if (mode === "backfill_months") return `Backfill ${backfillMonths || 0} months`;
  if (mode === "latest_only") return "Latest available";
  return "Auto";
}

function formatScopeFromParams(params = {}) {
  const mode = params.mode || "date_range";
  const singleDate = params.date || "";
  const dateWindow = dateWindowForMode(mode, singleDate, params.date_from || "", params.date_to || "");
  return formatWindowLabel(mode, singleDate, dateWindow, params.rolling_days || 0, params.backfill_months || 0);
}

function jobStageLabel(job) {
  if (job?.progress?.cancel_requested && job?.status === "running") return "Stopping";
  if (job?.progress?.stage) return titleizeToken(job.progress.stage);
  if (job?.status === "queued") return "Waiting";
  return titleizeToken(job?.status);
}

function progressSnapshot(job) {
  const progress = job?.progress || {};
  const queued = Number(progress.queued_files || 0);
  const processed = Number(progress.processed_files || 0);
  const uploaded = Number(progress.uploaded || 0);
  const downloaded = Number(progress.downloaded || 0);
  const skipped = Number(progress.skipped || 0);
  const errors = Number(progress.errors || 0);
  const remaining = progress.remaining_files ?? Math.max(queued - processed, 0);
  const percent = Number.isFinite(Number(progress.progress_pct)) ? Number(progress.progress_pct) : queued > 0 ? Math.round((processed / queued) * 100) : 0;
  return {
    queued,
    processed,
    uploaded,
    downloaded,
    skipped,
    errors,
    remaining,
    percent: Math.max(0, Math.min(100, percent)),
  };
}

function jobProgressLabel(job) {
  const snapshot = progressSnapshot(job);
  if (snapshot.queued > 0) return `${snapshot.processed}/${snapshot.queued} files`;
  return jobStageLabel(job);
}

function summarizeFailure(item) {
  const text = item?.error || item?.message || "Transfer failed";
  return String(text).length > 150 ? `${String(text).slice(0, 147)}...` : String(text);
}

export function JobsPage() {
  const [camera, setCamera] = useState("ROS");
  const [variable, setVariable] = useState("RGB");
  const [timezone, setTimezone] = useState("local");
  const [mode, setMode] = useState("rolling_days");
  const [singleDate, setSingleDate] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [rollingDays, setRollingDays] = useState(7);
  const [backfillMonths, setBackfillMonths] = useState(3);
  const [fileSelection, setFileSelection] = useState("all");
  const [newestN, setNewestN] = useState(10);
  const [startTime, setStartTime] = useState("");
  const [endTime, setEndTime] = useState("");
  const [csvPolicy, setCsvPolicy] = useState("always_refresh");
  const [dryRun, setDryRun] = useState(false);
  const [verifyChecksum, setVerifyChecksum] = useState(false);
  const [runIsolated, setRunIsolated] = useState(false);
  const [selectedJobId, setSelectedJobId] = useState("");
  const [actionBusy, setActionBusy] = useState(false);
  const [actionNotice, setActionNotice] = useState({ tone: "info", text: "" });

  const cameras = useAsync(() => api.getRemoteCameras(), [], true);
  const variables = useAsync(() => (camera ? api.getRemoteVariables(camera) : Promise.resolve({ variables: [] })), [camera], !!camera);
  const jobs = useAsync(() => api.listJobs(), [], true);
  const selectedJobQuery = useAsync(() => (selectedJobId ? api.getJob(selectedJobId) : Promise.resolve(null)), [selectedJobId], !!selectedJobId);
  const failures = useAsync(() => (selectedJobId ? api.getJobFailures(selectedJobId, 24) : Promise.resolve(null)), [selectedJobId], !!selectedJobId);
  const events = useAsync(() => (selectedJobId ? api.getJobEvents(selectedJobId) : Promise.resolve([])), [selectedJobId], !!selectedJobId);

  useEffect(() => {
    if (cameras.data?.cameras?.length && !camera) {
      setCamera(cameras.data.cameras[0]);
    }
  }, [cameras.data, camera]);

  useEffect(() => {
    if (variables.data?.variables?.length) {
      setVariable((prev) => (variables.data.variables.includes(prev) ? prev : variables.data.variables[0]));
    }
  }, [variables.data]);

  useEffect(() => {
    if (jobs.data?.length && !jobs.data.some((job) => job.id === selectedJobId)) {
      setSelectedJobId(jobs.data[0].id);
    }
  }, [jobs.data, selectedJobId]);

  const selectedJob = useMemo(() => {
    if (selectedJobQuery.data) return selectedJobQuery.data;
    return (jobs.data || []).find((job) => job.id === selectedJobId) || null;
  }, [jobs.data, selectedJobId, selectedJobQuery.data]);

  useEffect(() => {
    const refreshMs = selectedJob?.status === "running" || selectedJob?.progress?.cancel_requested ? 4000 : 10000;
    const id = setInterval(() => {
      jobs.run().catch(() => {});
      if (selectedJobId) {
        selectedJobQuery.run().catch(() => {});
        failures.run().catch(() => {});
        events.run().catch(() => {});
      }
    }, refreshMs);
    return () => clearInterval(id);
  }, [jobs.run, selectedJobId, selectedJobQuery.run, failures.run, events.run, selectedJob?.status, selectedJob?.progress?.cancel_requested]);

  const uploadModeSupported = mode !== "backfill_months";
  const dateWindow = dateWindowForMode(mode, singleDate, dateFrom, dateTo);
  const windowLabel = formatWindowLabel(mode, singleDate, dateWindow, rollingDays, backfillMonths);

  const idBase = useMemo(
    () =>
      scopeDigest({
        camera,
        variable,
        timezone,
        mode,
        singleDate,
        dateFrom,
        dateTo,
        rollingDays,
        backfillMonths,
        fileSelection,
        newestN,
        startTime,
        endTime,
        csvPolicy,
        dryRun,
        verifyChecksum,
        runIsolated,
      }),
    [camera, variable, timezone, mode, singleDate, dateFrom, dateTo, rollingDays, backfillMonths, fileSelection, newestN, startTime, endTime, csvPolicy, dryRun, verifyChecksum, runIsolated]
  );

  const selectedSnapshot = progressSnapshot(selectedJob);
  const failureItems = failures.data?.items || [];
  const failureCount = Number(failures.data?.total_unique_failures || 0);
  const recentEvents = useMemo(() => [...(events.data || [])].slice(-8).reverse(), [events.data]);
  const isSelectedActive = selectedJob && ["queued", "running"].includes(selectedJob.status);
  const canResumeScope = selectedJob && RESUMABLE_KINDS.has(selectedJob.kind) && !isSelectedActive;
  const canRetryFailed = canResumeScope && failureCount > 0;

  const downloadScope = () => ({
    camera,
    variable,
    timezone,
    mode,
    date: mode === "single_date" ? singleDate || null : null,
    date_from: mode === "date_range" ? dateFrom || null : null,
    date_to: mode === "date_range" ? dateTo || null : null,
    rolling_days: mode === "rolling_days" ? Number(rollingDays) : null,
    backfill_months: mode === "backfill_months" ? Number(backfillMonths) : null,
    file_selection: fileSelection,
    newest_n: fileSelection === "newest_n" ? Number(newestN) : null,
    start_time: startTime || null,
    end_time: endTime || null,
    csv_policy: csvPolicy,
    dry_run: dryRun,
    verify_checksum: verifyChecksum,
  });

  const transferScope = () => ({
    ...downloadScope(),
    run_isolated: runIsolated,
  });

  const uploadScope = () => ({
    camera,
    variable,
    mode,
    date: mode === "single_date" ? singleDate || null : null,
    date_from: mode === "date_range" ? dateFrom || null : null,
    date_to: mode === "date_range" ? dateTo || null : null,
    rolling_days: mode === "rolling_days" ? Number(rollingDays) : null,
    dry_run: dryRun,
    verify_checksum: verifyChecksum,
    run_isolated: runIsolated,
  });

  const refreshSelectedViews = async (jobId = selectedJobId) => {
    await jobs.run().catch(() => {});
    if (jobId) {
      if (jobId === selectedJobId) {
        await Promise.allSettled([selectedJobQuery.run(), failures.run(), events.run()]);
        return;
      }
      const [jobResult, failureResult, eventResult] = await Promise.allSettled([
        api.getJob(jobId),
        api.getJobFailures(jobId, 24),
        api.getJobEvents(jobId),
      ]);
      if (jobResult.status === "fulfilled") selectedJobQuery.setData(jobResult.value);
      if (failureResult.status === "fulfilled") failures.setData(failureResult.value);
      if (eventResult.status === "fulfilled") events.setData(eventResult.value);
    }
  };

  const runAction = async (label, fn, successText) => {
    setActionBusy(true);
    setActionNotice({ tone: "info", text: `${label} in progress...` });
    try {
      const result = await fn();
      setActionNotice({ tone: "success", text: successText || `${label} completed.` });
      return result;
    } catch (error) {
      setActionNotice({ tone: "error", text: `${label} failed: ${String(error?.message || error)}` });
      throw error;
    } finally {
      setActionBusy(false);
    }
  };

  const createDownload = async () => {
    const job = await runAction("Download job", () => api.createDownloadJob({ ...downloadScope(), idempotency_key: `download:${idBase}` }), "Download queued.").catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const createUpload = async () => {
    if (!uploadModeSupported) {
      setActionNotice({ tone: "error", text: "Upload jobs do not support backfill_months. Use rolling_days, latest_only, single_date, or date_range." });
      return;
    }
    const job = await runAction("Upload job", () => api.createUploadJob({ ...uploadScope(), idempotency_key: `upload:${idBase}` }), "Upload queued.").catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const createTransfer = async () => {
    const job = await runAction(
      "Transfer job",
      () => api.createTransferJob({ ...transferScope(), idempotency_key: `transfer:${idBase}` }),
      "Transfer queued. Temp files will be removed after each successful upload."
    ).catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const createVerify = async () => {
    const job = await runAction(
      "Verify job",
      () =>
        api.createVerifyJob({
          source_a: "remote",
          source_b: "local",
          camera,
          variable,
          date_from: dateWindow.from || null,
          date_to: dateWindow.to || null,
          cadence_seconds: 15,
          idempotency_key: `verify:${idBase}`,
        }),
      "Verify queued."
    ).catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const runInventory = async () => {
    const job = await runAction("Inventory scan", () => api.createInventoryJob(camera, variable), "Inventory scan queued.").catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const stopSelected = async () => {
    if (!selectedJobId) return;
    const job = await runAction("Stop job", () => api.cancelJob(selectedJobId), "Stop requested. The worker will finish its current file and then stop.").catch(() => null);
    if (!job) return;
    await refreshSelectedViews(selectedJobId);
  };

  const resumeSelected = async ({ failedOnly }) => {
    if (!selectedJobId) return;
    const text = failedOnly ? "Retry queued for failed files." : "Resume queued for the original scope.";
    const job = await runAction(failedOnly ? "Retry failed files" : "Resume job", () => api.resumeJob(selectedJobId, { failedOnly }), text).catch(() => null);
    if (!job) return;
    setSelectedJobId(job.id);
    await refreshSelectedViews(job.id);
  };

  const progressHeadline = selectedJob
    ? selectedSnapshot.queued > 0
      ? `${selectedSnapshot.processed} of ${selectedSnapshot.queued} files processed`
      : jobStageLabel(selectedJob)
    : "Select a job";

  const progressSubhead = selectedJob
    ? selectedJob.progress?.cancel_requested && selectedJob.status === "running"
      ? "Stop requested. The worker will stop after the current file finishes."
      : selectedJob.progress?.current_file
        ? `${titleizeToken(selectedJob.progress?.current_activity || "working")} ${selectedJob.progress.current_file}`
        : `Scope: ${formatScopeFromParams(selectedJob.params || {})}`
    : "Choose a recent job to inspect progress and issues.";

  return (
    <>
      <div className="page-header page-header-jobs">
        <div>
          <p className="page-kicker">Execution</p>
          <h2 className="page-title">Transfer Builder</h2>
          <p className="page-subtitle">Start remote-to-FTP transfers, stop active runs, and retry only the files that failed. The page stays focused on progress and issues instead of raw logs.</p>
        </div>
        <div className="page-header-actions">
          <Link className="link-button secondary" to="/ftp">
            Open FTP
          </Link>
          <Link className="link-button secondary" to="/logs">
            Logs
          </Link>
        </div>
      </div>

      <Notice tone={actionNotice.tone} text={actionNotice.text} />
      {cameras.error ? <ErrorBlock error={cameras.error} onRetry={cameras.run} /> : null}
      {variables.error ? <ErrorBlock error={variables.error} onRetry={variables.run} /> : null}

      <div className="grid two transfer-builder-grid">
        <Card title="Transfer Scope" className="card-hero">
          <div className="controls controls-grid">
            <label>
              Camera
              <select value={camera} onChange={(event) => setCamera(event.target.value)}>
                {(cameras.data?.cameras || []).map((item) => (
                  <option value={item} key={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Variable
              <select value={variable} onChange={(event) => setVariable(event.target.value)}>
                {(variables.data?.variables || []).map((item) => (
                  <option value={item} key={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Timezone
              <select value={timezone} onChange={(event) => setTimezone(event.target.value)}>
                <option value="local">local</option>
                <option value="utc">utc</option>
              </select>
            </label>
            <label>
              Mode
              <select value={mode} onChange={(event) => setMode(event.target.value)}>
                <option value="single_date">single_date</option>
                <option value="date_range">date_range</option>
                <option value="rolling_days">rolling_days</option>
                <option value="backfill_months">backfill_months</option>
                <option value="latest_only">latest_only</option>
              </select>
            </label>
            {mode === "single_date" ? (
              <label>
                Date
                <input type="date" value={singleDate} onChange={(event) => setSingleDate(event.target.value)} />
              </label>
            ) : null}
            {mode === "date_range" ? (
              <>
                <label>
                  Date from
                  <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} />
                </label>
                <label>
                  Date to
                  <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} />
                </label>
              </>
            ) : null}
            {mode === "rolling_days" ? (
              <label>
                Rolling days
                <input type="number" min={1} value={rollingDays} onChange={(event) => setRollingDays(event.target.value)} />
              </label>
            ) : null}
            {mode === "backfill_months" ? (
              <label>
                Backfill months
                <input type="number" min={1} value={backfillMonths} onChange={(event) => setBackfillMonths(event.target.value)} />
              </label>
            ) : null}
            <label className="checkbox-row">
              <input type="checkbox" checked={dryRun} onChange={(event) => setDryRun(event.target.checked)} /> Dry run
            </label>
          </div>

          <p className="transfer-scope-note">
            Transfer uses a temp workspace only. Files are uploaded to FTP and then removed from the local temp folder after success.
          </p>

          <div className="pill-row">
            <span className="tag">camera: {camera}</span>
            <span className="tag">variable: {variable}</span>
            <span className="tag">window: {windowLabel}</span>
            <span className="tag">local copy: temp only</span>
          </div>

          <details className="summary-disclosure" style={{ marginTop: "0.9rem" }}>
            <summary>
              <span>Advanced options</span>
              <span className="summary-disclosure-hint">Optional</span>
            </summary>
            <div className="summary-disclosure-body">
              <div className="controls controls-grid">
                <label>
                  File selection
                  <select value={fileSelection} onChange={(event) => setFileSelection(event.target.value)}>
                    <option value="all">all</option>
                    <option value="newest_only">newest_only</option>
                    <option value="newest_n">newest_n</option>
                  </select>
                </label>
                <label>
                  Newest N
                  <input type="number" min={1} value={newestN} disabled={fileSelection !== "newest_n"} onChange={(event) => setNewestN(event.target.value)} />
                </label>
                <label>
                  Start time
                  <input type="time" value={startTime} onChange={(event) => setStartTime(event.target.value)} />
                </label>
                <label>
                  End time
                  <input type="time" value={endTime} onChange={(event) => setEndTime(event.target.value)} />
                </label>
                <label>
                  CSV policy
                  <select value={csvPolicy} onChange={(event) => setCsvPolicy(event.target.value)}>
                    <option value="always_refresh">always_refresh</option>
                    <option value="remote_newer">remote_newer</option>
                    <option value="scheduled_refresh">scheduled_refresh</option>
                    <option value="never_refresh">never_refresh</option>
                  </select>
                </label>
                <label className="checkbox-row">
                  <input type="checkbox" checked={verifyChecksum} onChange={(event) => setVerifyChecksum(event.target.checked)} /> Verify checksum
                </label>
                <label className="checkbox-row">
                  <input type="checkbox" checked={runIsolated} onChange={(event) => setRunIsolated(event.target.checked)} /> Isolated FTP namespace
                </label>
              </div>
            </div>
          </details>
        </Card>

        <Card title="Run Transfer">
          <p className="transfer-builder-copy">Most runs only need scope selection and one button: start transfer. When something breaks, use the selected job panel below to stop or retry only the failed files.</p>

          <div className="controls">
            <button disabled={actionBusy} onClick={createTransfer}>
              Start Transfer
            </button>
            <button className="secondary" disabled={actionBusy} onClick={createDownload}>
              Download Only
            </button>
          </div>

          <details className="summary-disclosure" style={{ marginTop: "0.9rem" }}>
            <summary>
              <span>Secondary actions</span>
              <span className="summary-disclosure-hint">Optional</span>
            </summary>
            <div className="summary-disclosure-body">
              <div className="controls">
                <button className="secondary" disabled={actionBusy || !uploadModeSupported} onClick={createUpload}>
                  Upload Local Only
                </button>
                <button className="secondary" disabled={actionBusy} onClick={createVerify}>
                  Verify Local
                </button>
                <button className="secondary" disabled={actionBusy} onClick={runInventory}>
                  Inventory Scan
                </button>
              </div>
            </div>
          </details>
        </Card>
      </div>

      {jobs.loading && !jobs.data ? <Loading text="Loading jobs..." /> : null}
      {jobs.error ? <ErrorBlock error={jobs.error} onRetry={jobs.run} /> : null}

      <div className="grid two two-left-wide">
        <Card title="Recent Jobs" actions={<button className="secondary" onClick={() => refreshSelectedViews()}>Refresh</button>}>
          {jobs.data?.length ? (
            <VirtualList
              className="jobs-queue-list"
              items={jobs.data}
              height={380}
              rowHeight={96}
              renderItem={(job) => (
                <button
                  key={job.id}
                  type="button"
                  className={`result-row queue-row ${selectedJobId === job.id ? "selected" : ""}`}
                  onClick={() => setSelectedJobId(job.id)}
                >
                  <div className="queue-row-main">
                    <div className="queue-row-title">
                      <span className="mono queue-row-id">{shortId(job.id)}</span>
                      <span className="queue-row-kind">{titleizeToken(job.kind)}</span>
                    </div>
                    <span className={`tag queue-row-status ${queueStatusTone(job.status)}`}>{titleizeToken(job.status)}</span>
                  </div>
                  <div className="queue-row-meta">
                    <div className="queue-meta-item">
                      <span className="queue-meta-label">Progress</span>
                      <span className="queue-meta-value">{jobProgressLabel(job)}</span>
                    </div>
                    <div className="queue-meta-item">
                      <span className="queue-meta-label">Stage</span>
                      <span className="queue-meta-value">{jobStageLabel(job)}</span>
                    </div>
                    <div className="queue-meta-item">
                      <span className="queue-meta-label">Created</span>
                      <span className="mono queue-meta-value">{formatTimestamp(job.created_at)}</span>
                    </div>
                  </div>
                </button>
              )}
            />
          ) : (
            <Empty text="No jobs yet." />
          )}
        </Card>

        <div className="detail-stack">
          <Card title="Selected Job" actions={<button className="secondary" onClick={() => refreshSelectedViews()}>Refresh</button>}>
            {selectedJob ? (
              <>
                <div className="job-summary-head">
                  <div>
                    <p className="job-summary-kicker">Selected Run</p>
                    <div className="job-summary-id mono">{selectedJob.id}</div>
                  </div>
                  <div className="job-summary-badges">
                    <span className={`tag ${queueStatusTone(selectedJob.status)}`}>{titleizeToken(selectedJob.status)}</span>
                    <span className="tag">{titleizeToken(selectedJob.kind)}</span>
                  </div>
                </div>

                <div className="job-focus-actions">
                  <button className="secondary" disabled={actionBusy || !isSelectedActive} onClick={stopSelected}>
                    Stop
                  </button>
                  <button className="secondary" disabled={actionBusy || !canRetryFailed} onClick={() => resumeSelected({ failedOnly: true })}>
                    Retry Failed
                  </button>
                  <button className="secondary" disabled={actionBusy || !canResumeScope} onClick={() => resumeSelected({ failedOnly: false })}>
                    Resume Scope
                  </button>
                </div>

                <div className="job-progress-panel">
                  <div className="job-progress-copy">
                    <strong>{progressHeadline}</strong>
                    <span>{progressSubhead}</span>
                  </div>
                  <div className="job-progress-track">
                    <span className="job-progress-fill" style={{ width: `${selectedSnapshot.percent}%` }} />
                  </div>
                </div>

                <div className="job-summary-facts">
                  <div className="job-summary-fact">
                    <span>Processed</span>
                    <strong>{selectedSnapshot.queued > 0 ? `${selectedSnapshot.processed}/${selectedSnapshot.queued}` : "-"}</strong>
                  </div>
                  <div className="job-summary-fact">
                    <span>Uploaded</span>
                    <strong>{selectedSnapshot.uploaded}</strong>
                  </div>
                  <div className="job-summary-fact">
                    <span>Errors</span>
                    <strong>{selectedSnapshot.errors}</strong>
                  </div>
                  <div className="job-summary-fact">
                    <span>Remaining</span>
                    <strong>{selectedSnapshot.queued > 0 ? selectedSnapshot.remaining : "-"}</strong>
                  </div>
                  <div className="job-summary-fact">
                    <span>Stage</span>
                    <strong>{jobStageLabel(selectedJob)}</strong>
                  </div>
                  <div className="job-summary-fact">
                    <span>Scope</span>
                    <strong>{formatScopeFromParams(selectedJob.params || {})}</strong>
                  </div>
                </div>

                <div className="pill-row" style={{ marginTop: "0.9rem" }}>
                  <span className="tag">camera: {selectedJob.params?.camera || "-"}</span>
                  <span className="tag">variable: {selectedJob.params?.variable || "-"}</span>
                  <span className="tag">created: {formatTimestamp(selectedJob.created_at)}</span>
                </div>

                {selectedJob.error_summary ? (
                  <div className="job-summary-error" style={{ marginTop: "0.9rem" }}>
                    <span className="job-summary-error-label">Job error</span>
                    <p>{selectedJob.error_summary}</p>
                  </div>
                ) : null}
              </>
            ) : (
              <Empty text="Select a job to inspect progress and issues." />
            )}
          </Card>

          <Card title="Issues">
            {!selectedJobId ? (
              <Empty text="Select a job to see failures." />
            ) : failures.loading && !failures.data ? (
              <Loading text="Loading failures..." />
            ) : failures.error ? (
              <ErrorBlock error={failures.error} onRetry={failures.run} />
            ) : failureItems.length ? (
              <div className="job-failure-list">
                <div className="job-failure-summary">
                  <strong>{failureCount} unique failed files</strong>
                  <span>Use Retry Failed to re-run only these files.</span>
                </div>
                {failureItems.map((item) => (
                  <div className="job-failure-row" key={`${item.date}-${item.filename}`}>
                    <div className="job-failure-head">
                      <strong className="mono">{item.filename}</strong>
                      <span>{item.date || "-"}</span>
                    </div>
                    <p>{summarizeFailure(item)}</p>
                  </div>
                ))}
              </div>
            ) : (
              <Empty text="No failed files recorded for this job." />
            )}
          </Card>

          <Card title="Recent Activity">
            {!selectedJobId ? (
              <Empty text="Select a job to inspect activity." />
            ) : selectedJobQuery.loading && !selectedJob ? (
              <Loading text="Loading job details..." />
            ) : events.error ? (
              <ErrorBlock error={events.error} onRetry={events.run} />
            ) : recentEvents.length ? (
              <div className="event-stream-list compact">
                {recentEvents.map((event, index) => (
                  <div className="event-stream-row compact" key={`${event.created_at}-${index}`}>
                    <div className="event-stream-meta">
                      <span className="mono">{formatTimestamp(event.created_at)}</span>
                      <span className={`tag ${event.level === "ERROR" ? "alert" : event.level === "INFO" ? "ok" : ""}`}>{event.level}</span>
                    </div>
                    <p className="event-stream-message">{event.message}</p>
                  </div>
                ))}
              </div>
            ) : (
              <Empty text="No recent events for this job." />
            )}
          </Card>
        </div>
      </div>
    </>
  );
}

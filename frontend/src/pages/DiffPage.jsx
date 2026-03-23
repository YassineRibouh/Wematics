import { useEffect, useMemo, useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading } from "../components/UiBits";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

const SOURCES = ["remote", "local", "ftp"];

export function DiffPage() {
  const [sourceA, setSourceA] = useState("remote");
  const [sourceB, setSourceB] = useState("local");
  const [camera, setCamera] = useState("");
  const [variable, setVariable] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [cadence, setCadence] = useState(15);

  const cameras = useAsync(() => api.getRemoteCameras(), [], true);
  const variables = useAsync(() => (camera ? api.getRemoteVariables(camera) : Promise.resolve({ variables: [] })), [camera], !!camera);
  const diff = useAsync(
    () =>
      camera && variable
        ? api.computeDiff({
            source_a: sourceA,
            source_b: sourceB,
            camera,
            variable,
            date_from: dateFrom || null,
            date_to: dateTo || null,
            cadence_seconds: Number(cadence),
          })
        : Promise.resolve(null),
    [sourceA, sourceB, camera, variable, dateFrom, dateTo, cadence],
    false
  );

  useEffect(() => {
    if (!camera && cameras.data?.cameras?.length) {
      setCamera(cameras.data.cameras[0]);
    }
  }, [camera, cameras.data]);

  useEffect(() => {
    if (variables.data?.variables?.length) {
      setVariable((prev) => (variables.data.variables.includes(prev) ? prev : variables.data.variables[0]));
    } else {
      setVariable("");
    }
  }, [variables.data]);

  const exportUrl = useMemo(() => {
    if (!camera || !variable) return "";
    const params = new URLSearchParams({
      source_a: sourceA,
      source_b: sourceB,
      camera,
      variable,
      cadence_seconds: String(cadence),
    });
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    return `${import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api"}/diff/export-csv?${params.toString()}`;
  }, [sourceA, sourceB, camera, variable, dateFrom, dateTo, cadence]);

  return (
    <>
      <div>
        <h2 className="page-title">Diff & Gap Analysis</h2>
        <p className="page-subtitle">Compare timelines and identify missing dates, partial days, and intervals.</p>
      </div>

      <Card title="Comparison Setup">
        <div className="controls">
          <label>
            Source A
            <select value={sourceA} onChange={(e) => setSourceA(e.target.value)}>
              {SOURCES.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Source B
            <select value={sourceB} onChange={(e) => setSourceB(e.target.value)}>
              {SOURCES.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Camera
            <select value={camera} onChange={(e) => setCamera(e.target.value)}>
              {(cameras.data?.cameras || []).map((item) => (
                <option value={item} key={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Variable
            <select value={variable} onChange={(e) => setVariable(e.target.value)}>
              {(variables.data?.variables || []).map((item) => (
                <option value={item} key={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Date from
            <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
          </label>
          <label>
            Date to
            <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </label>
          <label>
            Cadence sec
            <input type="number" value={cadence} min={1} onChange={(e) => setCadence(e.target.value)} />
          </label>
          <button onClick={() => diff.run()}>Compute Diff</button>
          {exportUrl ? (
            <a href={exportUrl} target="_blank" rel="noreferrer">
              <button className="secondary">Export CSV</button>
            </a>
          ) : null}
        </div>
      </Card>

      {diff.loading ? <Loading text="Computing gaps..." /> : null}
      {diff.error ? <ErrorBlock error={diff.error} onRetry={diff.run} /> : null}

      {diff.data ? (
        <>
          <div className="grid two">
            <Card title="Summary">
              <p className="mono">Dates in A: {diff.data.summary?.dates_in_source_a ?? 0}</p>
              <p className="mono">Dates in B: {diff.data.summary?.dates_in_source_b ?? 0}</p>
              <p className="mono">Missing dates: {diff.data.summary?.missing_dates_count ?? 0}</p>
              <p className="mono">Partial days: {diff.data.summary?.partial_days_count ?? 0}</p>
              <p className="mono">Latest A: {diff.data.summary?.latest_source_a || "-"}</p>
              <p className="mono">Latest B: {diff.data.summary?.latest_source_b || "-"}</p>
            </Card>

            <Card title="Missing Dates">
              {diff.data.missing_dates?.length ? (
                <div className="mono">{diff.data.missing_dates.join(", ")}</div>
              ) : (
                <Empty text="No missing dates found." />
              )}
            </Card>
          </div>

          <Card title="Gap List">
            {diff.data.gap_rows?.length ? (
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Missing Start</th>
                    <th>Missing End</th>
                    <th>Expected</th>
                    <th>Observed</th>
                    <th>Completeness %</th>
                    <th>Missing Count</th>
                  </tr>
                </thead>
                <tbody>
                  {diff.data.gap_rows.map((row, idx) => (
                    <tr key={`${row.date}-${idx}`}>
                      <td className="mono">{row.date}</td>
                      <td className="mono">{row.missing_start || "-"}</td>
                      <td className="mono">{row.missing_end || "-"}</td>
                      <td>{row.expected_count}</td>
                      <td>{row.observed_count}</td>
                      <td>{row.completeness_pct}</td>
                      <td>{row.missing_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <Empty text="No partial gaps found for this filter." />
            )}
          </Card>
        </>
      ) : null}
    </>
  );
}


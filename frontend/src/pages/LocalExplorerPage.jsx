import { useMemo, useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading, Notice } from "../components/UiBits";
import { api } from "../lib/api";
import { formatBytes, isImageFilename } from "../lib/format";
import { useAsync } from "../lib/useAsync";
import { useDebouncedValue } from "../lib/useDebouncedValue";

export function LocalExplorerPage() {
  const [camera, setCamera] = useState("");
  const [variable, setVariable] = useState("");
  const [selectedDate, setSelectedDate] = useState("");
  const [search, setSearch] = useState("");
  const [scanBusy, setScanBusy] = useState(false);
  const [scanMessage, setScanMessage] = useState("");
  const debouncedSearch = useDebouncedValue(search, 250);

  const dates = useAsync(() => api.getLocalDates(camera, variable), [camera, variable], true);
  const summary = useAsync(() => api.getStorageSummary(camera, variable), [camera, variable], true);
  const files = useAsync(
    () =>
      camera && variable && selectedDate
        ? api.getLocalFiles({ camera, variable, date: selectedDate, search: debouncedSearch })
        : Promise.resolve(null),
    [camera, variable, selectedDate, debouncedSearch],
    !!(camera && variable && selectedDate)
  );

  const allDates = dates.data?.dates || [];
  const cameras = useMemo(() => [...new Set(allDates.map((item) => item.camera))], [allDates]);
  const variables = useMemo(
    () => [...new Set(allDates.filter((item) => !camera || item.camera === camera).map((item) => item.variable))],
    [allDates, camera]
  );

  const filteredDates = useMemo(
    () => allDates.filter((item) => (!camera || item.camera === camera) && (!variable || item.variable === variable)),
    [allDates, camera, variable]
  );

  const triggerScan = async () => {
    setScanBusy(true);
    setScanMessage("Running incremental scan...");
    try {
      await api.triggerLocalScan(camera, variable);
      await Promise.all([dates.run(), summary.run()]);
      setScanMessage("Incremental scan triggered and inventory refreshed.");
    } catch (error) {
      setScanMessage(`Scan failed: ${String(error?.message || error)}`);
    } finally {
      setScanBusy(false);
    }
  };

  return (
    <>
      <div>
        <h2 className="page-title">Local Explorer</h2>
        <p className="page-subtitle">Cached archive inventory with incremental scan and per-day browsing.</p>
      </div>

      <Card
        title="Filters"
        actions={
          <button type="button" disabled={scanBusy} onClick={triggerScan}>
            Scan Incremental
          </button>
        }
      >
        <Notice tone={scanMessage.startsWith("Scan failed") ? "error" : "info"} text={scanMessage} />
        <div className="controls">
          <label>
            Camera
            <select value={camera} onChange={(e) => setCamera(e.target.value)}>
              <option value="">All</option>
              {cameras.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Variable
            <select value={variable} onChange={(e) => setVariable(e.target.value)}>
              <option value="">All</option>
              {variables.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            File Search
            <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="filename contains..." />
          </label>
        </div>
      </Card>

      {dates.loading && !dates.data ? <Loading text="Building local inventory..." /> : null}
      {dates.error ? <ErrorBlock error={dates.error} onRetry={dates.run} /> : null}

      <div className="grid two">
        <Card title="Date Inventory">
          {filteredDates.length ? (
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Camera</th>
                  <th>Variable</th>
                  <th>Count</th>
                  <th>Size</th>
                  <th>Last Modified</th>
                </tr>
              </thead>
              <tbody>
                {filteredDates.slice(0, 400).map((item) => (
                  <tr
                    key={`${item.camera}-${item.variable}-${item.date}`}
                    className={selectedDate === item.date && camera === item.camera && variable === item.variable ? "selected-row" : ""}
                    onClick={() => {
                      setCamera(item.camera);
                      setVariable(item.variable);
                      setSelectedDate(item.date);
                    }}
                    style={{ cursor: "pointer" }}
                  >
                    <td className="mono">{item.date}</td>
                    <td>{item.camera}</td>
                    <td>{item.variable}</td>
                    <td>{item.file_count}</td>
                    <td>{formatBytes(item.total_size)}</td>
                    <td className="mono">{item.last_modified || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Empty text="No local inventory available yet. Trigger a scan first." />
          )}
        </Card>

        <Card title="Storage Summary">
          <p className="mono">Dates: {summary.data?.dates ?? "-"}</p>
          <p className="mono">Files: {summary.data?.files ?? "-"}</p>
          <p className="mono">Size: {formatBytes(summary.data?.bytes ?? 0)}</p>
          <p className="mono">Last scan: {summary.data?.last_scan_at ?? "-"}</p>
          <hr />
          <p className="mono">Selected date: {selectedDate || "-"}</p>
        </Card>
      </div>

      <Card title="Files in Selected Date">
        {camera && variable && selectedDate ? (
          files.loading && !files.data ? (
            <Loading text="Loading files..." />
          ) : files.error ? (
            <ErrorBlock error={files.error} onRetry={files.run} />
          ) : files.data?.items?.length ? (
            <table>
              <thead>
                <tr>
                  <th>Filename</th>
                  <th>Timestamp</th>
                  <th>Size</th>
                  <th>Preview</th>
                </tr>
              </thead>
              <tbody>
                {files.data.items.map((item) => (
                  <tr key={item.filename}>
                    <td className="mono">{item.filename}</td>
                    <td className="mono">{item.parsed_timestamp || "-"}</td>
                    <td>{formatBytes(item.file_size || 0)}</td>
                    <td>
                      {isImageFilename(item.filename) ? (
                        <a
                          href={`${
                            import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api"
                          }/local/preview?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(
                            variable
                          )}&date=${encodeURIComponent(selectedDate)}&filename=${encodeURIComponent(item.filename)}`}
                          target="_blank"
                          rel="noreferrer"
                        >
                          open
                        </a>
                      ) : (
                        "-"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Empty text="No files found for this date/filter." />
          )
        ) : (
          <Empty text="Select a date row to view files." />
        )}
      </Card>
    </>
  );
}

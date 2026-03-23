import { useMemo, useState } from "react";
import { Card } from "../components/Card";
import { VirtualList } from "../components/VirtualList";
import { Empty, ErrorBlock, Loading } from "../components/UiBits";
import { api } from "../lib/api";
import { formatBytes } from "../lib/format";
import { useAsync } from "../lib/useAsync";
import { useDebouncedValue } from "../lib/useDebouncedValue";

export function FtpExplorerPage() {
  const [tab, setTab] = useState("tracked");

  const [camera, setCamera] = useState("");
  const [variable, setVariable] = useState("");
  const [selectedDate, setSelectedDate] = useState("");
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebouncedValue(search, 250);

  const [ftpPath, setFtpPath] = useState("/");
  const [ftpPathInput, setFtpPathInput] = useState("/");

  const dates = useAsync(() => api.getFtpDates(camera, variable), [camera, variable], tab === "tracked");
  const files = useAsync(
    () =>
      camera && variable && selectedDate
        ? api.getFtpFiles({ camera, variable, date: selectedDate, search: debouncedSearch })
        : Promise.resolve(null),
    [camera, variable, selectedDate, debouncedSearch],
    !!(camera && variable && selectedDate && tab === "tracked")
  );
  const server = useAsync(() => api.getFtpServerList(ftpPath), [ftpPath], tab === "server");

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

  return (
    <>
      <div>
        <h2 className="page-title">FTP Explorer</h2>
        <p className="page-subtitle">Inspect tracked uploads and browse the FTP server directly, including other-source data.</p>
      </div>

      <Card
        title="View Mode"
        actions={
          <div className="controls">
            <button className={tab === "tracked" ? "" : "secondary"} onClick={() => setTab("tracked")}>
              Tracked Uploads
            </button>
            <button className={tab === "server" ? "" : "secondary"} onClick={() => setTab("server")}>
              FTP Server Browser
            </button>
          </div>
        }
      >
        {tab === "tracked" ? (
          <p className="mono">Use this view for files tracked by Wematics upload jobs (DB-backed inventory).</p>
        ) : (
          <p className="mono">Use this view to inspect any FTP folder, including non-Wematics or external-source data.</p>
        )}
      </Card>

      {tab === "tracked" ? (
        <>
          <Card title="Filters" actions={<button onClick={() => dates.run()}>Refresh</button>}>
            <div className="controls">
              <label>
                Camera
                <select value={camera} onChange={(e) => setCamera(e.target.value)}>
                  <option value="">All</option>
                  {cameras.map((item) => (
                    <option value={item} key={item}>
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
                    <option value={item} key={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Search Filename
                <input value={search} onChange={(e) => setSearch(e.target.value)} />
              </label>
            </div>
          </Card>

          {dates.loading && !dates.data ? <Loading text="Loading FTP timeline..." /> : null}
          {dates.error ? <ErrorBlock error={dates.error} onRetry={dates.run} /> : null}

          <div className="grid two">
            <Card title="FTP Dates">
              {filteredDates.length ? (
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Camera</th>
                      <th>Variable</th>
                      <th>Count</th>
                      <th>Size</th>
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
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <Empty text="No FTP records yet. Run an upload job first." />
              )}
            </Card>

            <Card title="Selected Date Details">
              <p className="mono">Camera: {camera || "-"}</p>
              <p className="mono">Variable: {variable || "-"}</p>
              <p className="mono">Date: {selectedDate || "-"}</p>
              <p className="mono">Rows: {files.data?.total ?? 0}</p>
            </Card>
          </div>

          <Card title="FTP Files (Tracked)">
            {camera && variable && selectedDate ? (
              files.loading && !files.data ? (
                <Loading text="Loading FTP files..." />
              ) : files.error ? (
                <ErrorBlock error={files.error} onRetry={files.run} />
              ) : files.data?.items?.length ? (
                <table>
                  <thead>
                    <tr>
                      <th>Filename</th>
                      <th>Timestamp</th>
                      <th>Size</th>
                      <th>FTP Path</th>
                    </tr>
                  </thead>
                  <tbody>
                    {files.data.items.map((item) => (
                      <tr key={item.filename}>
                        <td className="mono">{item.filename}</td>
                        <td className="mono">{item.parsed_timestamp || "-"}</td>
                        <td>{formatBytes(item.file_size || 0)}</td>
                        <td className="mono">{item.ftp_path || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <Empty text="No files for this date/filter." />
              )
            ) : (
              <Empty text="Select a date from FTP timeline to inspect tracked files." />
            )}
          </Card>
        </>
      ) : (
        <>
          <Card title="FTP Server Path">
            <div className="controls">
              <label style={{ minWidth: "340px", flex: "1 1 340px" }}>
                Path
                <input value={ftpPathInput} onChange={(e) => setFtpPathInput(e.target.value)} placeholder="/incoming" />
              </label>
              <button onClick={() => setFtpPath(ftpPathInput || "/")}>Go</button>
              <button className="secondary" onClick={() => setFtpPath(server.data?.parent || "/")}>
                Up
              </button>
              <button className="secondary" onClick={() => server.run()}>
                Refresh
              </button>
              <button className="secondary" onClick={() => { setFtpPath("/"); setFtpPathInput("/"); }}>
                /
              </button>
              <button className="secondary" onClick={() => { setFtpPath("/images"); setFtpPathInput("/images"); }}>
                /images
              </button>
              <button className="secondary" onClick={() => { setFtpPath("/wematics"); setFtpPathInput("/wematics"); }}>
                /wematics
              </button>
            </div>
            <p className="mono">Current: {server.data?.path || ftpPath}</p>
          </Card>

          {server.loading && !server.data ? <Loading text="Loading FTP server listing..." /> : null}
          {server.error ? <ErrorBlock error={server.error} onRetry={server.run} /> : null}

          <Card title="FTP Directory Listing">
            {server.data?.entries?.length ? (
              <>
                {server.data.truncated ? <p className="mono">Listing truncated. Narrow to a deeper path for full results.</p> : null}
                <VirtualList
                  items={server.data.entries}
                  height={430}
                  rowHeight={42}
                  renderItem={(entry) => (
                    <div
                      key={`${entry.path}-${entry.name}`}
                      className="virtual-row"
                      style={{ gridTemplateColumns: "1.2fr 70px 90px 170px 1.5fr 90px", cursor: entry.type === "dir" ? "pointer" : "default" }}
                      onClick={() => {
                        if (entry.type === "dir") {
                          setFtpPath(entry.path);
                          setFtpPathInput(entry.path);
                        }
                      }}
                    >
                      <span className="mono">{entry.name}</span>
                      <span>{entry.type}</span>
                      <span>{entry.size === null || entry.size === undefined ? "-" : formatBytes(entry.size)}</span>
                      <span className="mono">{entry.modified || "-"}</span>
                      <span className="mono">{entry.path}</span>
                      <span>
                        {entry.type === "file" ? (
                          <a href={api.getFtpServerDownloadUrl(entry.path)} target="_blank" rel="noreferrer">
                            download
                          </a>
                        ) : (
                          "-"
                        )}
                      </span>
                    </div>
                  )}
                />
              </>
            ) : (
              <Empty text="No entries found at this FTP path." />
            )}
          </Card>
        </>
      )}
    </>
  );
}

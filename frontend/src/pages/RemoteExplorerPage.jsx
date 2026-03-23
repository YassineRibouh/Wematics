import { useEffect, useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card } from "../components/Card";
import { VirtualList } from "../components/VirtualList";
import { Empty, ErrorBlock, Loading } from "../components/UiBits";
import { api } from "../lib/api";
import { formatNumber, isCsvFilename, isImageFilename } from "../lib/format";
import { useAsync } from "../lib/useAsync";

function classifyFile(name) {
  if (isImageFilename(name)) return "Image";
  if (isCsvFilename(name)) return "CSV";
  return "Other";
}

export function RemoteExplorerPage() {
  const [camera, setCamera] = useState("");
  const [variable, setVariable] = useState("");
  const [timezone, setTimezone] = useState("local");
  const [date, setDate] = useState("");
  const [preview, setPreview] = useState("");
  const [selectedCsv, setSelectedCsv] = useState("");
  const [plotTimeColumn, setPlotTimeColumn] = useState("");
  const [plotValueColumn, setPlotValueColumn] = useState("");

  const cameras = useAsync(() => api.getRemoteCameras(), [], true);
  const variables = useAsync(() => (camera ? api.getRemoteVariables(camera) : Promise.resolve({ variables: [] })), [camera], !!camera);
  const dates = useAsync(
    () => (camera && variable ? api.getRemoteDates(camera, variable, timezone) : Promise.resolve({ dates: [] })),
    [camera, variable, timezone],
    !!(camera && variable)
  );
  const files = useAsync(
    () => (camera && variable && date ? api.getRemoteFiles(camera, variable, date, timezone) : Promise.resolve(null)),
    [camera, variable, date, timezone],
    !!(camera && variable && date)
  );

  const csvFiles = useMemo(
    () => (files.data?.files || []).filter((name) => isCsvFilename(name)),
    [files.data]
  );
  const hourlyImages = useMemo(() => files.data?.images_hourly || [], [files.data]);
  const isImageMode = Boolean(files.data?.image_preview_mode);
  const sampleEnabled = Boolean(camera && variable && date && selectedCsv && files.data && !isImageMode);
  const analysisEnabled = Boolean(camera && variable && date && selectedCsv && files.data && !isImageMode);

  const sample = useAsync(
    () =>
      camera && variable && date && selectedCsv
        ? api.getRemoteFileSample(camera, variable, date, selectedCsv, timezone, 8)
        : Promise.resolve(null),
    [camera, variable, date, selectedCsv, timezone],
    sampleEnabled
  );

  const analysis = useAsync(
    () =>
      camera && variable && date && selectedCsv
        ? api.getRemoteFileAnalysis(
            camera,
            variable,
            date,
            selectedCsv,
            timezone,
            3000,
            plotTimeColumn,
            plotValueColumn
          )
        : Promise.resolve(null),
    [camera, variable, date, selectedCsv, timezone, plotTimeColumn, plotValueColumn],
    analysisEnabled
  );

  useEffect(() => {
    if (!camera && cameras.data?.cameras?.length) {
      setCamera(cameras.data.cameras[0]);
    }
  }, [cameras.data, camera]);

  useEffect(() => {
    if (camera && variables.data?.variables?.length) {
      setVariable((prev) => (variables.data.variables.includes(prev) ? prev : variables.data.variables[0]));
    } else {
      setVariable("");
    }
  }, [camera, variables.data]);

  useEffect(() => {
    if (dates.data?.dates?.length) {
      setDate((prev) => (dates.data.dates.includes(prev) ? prev : dates.data.dates[dates.data.dates.length - 1]));
    } else {
      setDate("");
    }
  }, [dates.data]);

  useEffect(() => {
    if (isImageMode) {
      setSelectedCsv("");
      setPlotTimeColumn("");
      setPlotValueColumn("");
      return;
    }
    const firstCsv = csvFiles[0] || "";
    setSelectedCsv((prev) => (prev && csvFiles.includes(prev) ? prev : firstCsv));
  }, [csvFiles, isImageMode]);

  useEffect(() => {
    setPlotTimeColumn("");
    setPlotValueColumn("");
  }, [selectedCsv]);

  useEffect(() => {
    if (!isImageMode) {
      setPreview("");
      return;
    }
    setPreview((prev) => (prev && hourlyImages.includes(prev) ? prev : hourlyImages[0] || ""));
  }, [hourlyImages, isImageMode]);

  useEffect(() => {
    if (!analysis.data) return;
    if (!plotTimeColumn && analysis.data.suggested_time_column) {
      setPlotTimeColumn(analysis.data.suggested_time_column);
    }
    if (!plotValueColumn && analysis.data.suggested_value_column) {
      setPlotValueColumn(analysis.data.suggested_value_column);
    }
  }, [analysis.data, plotTimeColumn, plotValueColumn]);

  const distribution = useMemo(() => files.data?.time_distribution || [], [files.data]);
  const breakdown = files.data?.file_breakdown || { images: 0, csv: 0, other: 0 };
  const chartPoints = analysis.data?.plot?.points || [];
  const previewUrl = (name) =>
    `${import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api"}/remote/preview?camera=${encodeURIComponent(
      camera
    )}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(date)}&filename=${encodeURIComponent(name)}&timezone=${encodeURIComponent(
      timezone
    )}`;

  return (
    <>
      <div>
        <h2 className="page-title">Remote Explorer</h2>
        <p className="page-subtitle">Inspect Wematics timeline by day and preview hourly images or CSV time-series.</p>
      </div>

      <Card title="Selection">
        <div className="controls">
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
            Timezone
            <select value={timezone} onChange={(e) => setTimezone(e.target.value)}>
              <option value="local">local</option>
              <option value="utc">utc</option>
            </select>
          </label>
          <label>
            Date
            <select value={date} onChange={(e) => setDate(e.target.value)}>
              {(dates.data?.dates || []).map((item) => (
                <option value={item} key={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
        </div>
      </Card>

      {cameras.error ? <ErrorBlock error={cameras.error} onRetry={cameras.run} /> : null}
      {variables.error ? <ErrorBlock error={variables.error} onRetry={variables.run} /> : null}
      {dates.error ? <ErrorBlock error={dates.error} onRetry={dates.run} /> : null}

      {files.loading && !files.data ? <Loading text="Loading remote files..." /> : null}
      {files.error ? <ErrorBlock error={files.error} onRetry={files.run} /> : null}

      {files.data ? (
        <div className="grid two">
          <Card title="Day Summary">
            <p className="mono">Files: {files.data.count}</p>
            <p className="mono">Images: {breakdown.images}</p>
            <p className="mono">CSV: {breakdown.csv}</p>
            <p className="mono">Other: {breakdown.other}</p>
            <p>
              Preview mode:{" "}
              <span className={`tag ${isImageMode ? "ok" : ""}`}>
                {isImageMode ? "images (hourly)" : "csv/other"}
              </span>
            </p>
            <p className="mono">Earliest: {files.data.earliest || "-"}</p>
            <p className="mono">Latest: {files.data.latest || "-"}</p>
            <p className="mono">Timezone: {timezone}</p>
          </Card>

          <Card title="Per-Hour Distribution">
            {distribution.length ? (
              <div style={{ width: "100%", height: 240 }}>
                <ResponsiveContainer>
                  <AreaChart data={distribution}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="label" />
                    <YAxis allowDecimals={false} />
                    <Tooltip />
                    <Area type="monotone" dataKey="count" stroke="#005f8f" fill="#a7d2ef" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <Empty text="No timestamp distribution available for this day." />
            )}
          </Card>
        </div>
      ) : null}

      <Card title={isImageMode ? "Image Preview (1 per hour)" : "Files + CSV Analysis"}>
        {files.data?.files?.length ? (
          isImageMode ? (
            breakdown.images > 0 ? (
              <>
                <p className="mono">
                  Showing {hourlyImages.length} image(s), capped to one image per hour from {breakdown.images} total image file(s).
                </p>
                {hourlyImages.length ? (
                  <div className="thumb-grid">
                    {hourlyImages.map((name) => (
                      <button
                        key={name}
                        type="button"
                        className={`secondary ${preview === name ? "thumb-selected" : ""}`}
                        onClick={() => setPreview(name)}
                        title={name}
                        style={{ padding: 0 }}
                      >
                        <img src={previewUrl(name)} alt={name} />
                      </button>
                    ))}
                  </div>
                ) : (
                  <Empty text="Image files exist, but no timestamp could be parsed for hourly sampling." />
                )}
                {preview ? (
                  <div className="image-preview-panel">
                    <img src={previewUrl(preview)} alt={preview} />
                    <p className="mono">Selected: {preview}</p>
                  </div>
                ) : null}
              </>
            ) : (
              <Empty text="No image files found for this variable/day." />
            )
          ) : (
            <>
              <div className="grid two">
                <Card title="Remote Files (Selected Day)">
                  <VirtualList
                    items={files.data.files}
                    height={340}
                    rowHeight={38}
                    renderItem={(name) => (
                      <div
                        key={name}
                        className={`virtual-row ${selectedCsv === name ? "selected" : ""}`}
                        style={{ gridTemplateColumns: "1fr 100px", cursor: "pointer" }}
                        onClick={() => isCsvFilename(name) && setSelectedCsv(name)}
                      >
                        <span className="mono">{name}</span>
                        <span>{classifyFile(name)}</span>
                      </div>
                    )}
                  />
                </Card>

                <Card title="CSV Plot by Time">
                  {csvFiles.length ? (
                    <>
                      <div className="controls">
                        <label>
                          CSV file
                          <select value={selectedCsv} onChange={(e) => setSelectedCsv(e.target.value)}>
                            {csvFiles.map((name) => (
                              <option key={name} value={name}>
                                {name}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Time column
                          <select value={plotTimeColumn} onChange={(e) => setPlotTimeColumn(e.target.value)}>
                            <option value="">Auto</option>
                            {(analysis.data?.time_columns || []).map((item) => (
                              <option key={item.name} value={item.name}>
                                {item.name}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label>
                          Value column
                          <select value={plotValueColumn} onChange={(e) => setPlotValueColumn(e.target.value)}>
                            <option value="">Auto</option>
                            {(analysis.data?.numeric_columns || []).map((item) => (
                              <option key={item.name} value={item.name}>
                                {item.name}
                              </option>
                            ))}
                          </select>
                        </label>
                      </div>

                      {analysis.loading && !analysis.data ? (
                        <Loading text="Analyzing CSV for plottable time series..." />
                      ) : analysis.error ? (
                        <ErrorBlock error={analysis.error} onRetry={analysis.run} />
                      ) : analysis.data ? (
                        <>
                          <p className="mono">
                            Rows scanned: {formatNumber(analysis.data.rows_scanned)} | Time columns: {analysis.data.time_columns?.length || 0} |
                            Numeric columns: {analysis.data.numeric_columns?.length || 0}
                          </p>
                          {analysis.data.issues?.length ? (
                            <div className="error-block" style={{ marginBottom: "0.6rem" }}>
                              {analysis.data.issues.join(" ")}
                            </div>
                          ) : null}
                          {chartPoints.length ? (
                            <div style={{ width: "100%", height: 260 }}>
                              <ResponsiveContainer>
                                <LineChart data={chartPoints}>
                                  <CartesianGrid strokeDasharray="3 3" />
                                  <XAxis dataKey="time" hide />
                                  <YAxis />
                                  <Tooltip />
                                  <Line type="monotone" dataKey="value" stroke="#0d6a41" dot={false} strokeWidth={2} />
                                </LineChart>
                              </ResponsiveContainer>
                            </div>
                          ) : (
                            <Empty text="No plottable time-series points found in this CSV." />
                          )}
                        </>
                      ) : (
                        <Empty text="Select a CSV file to analyze." />
                      )}
                    </>
                  ) : (
                    <Empty text="No CSV files found for this variable/day." />
                  )}
                </Card>
              </div>

              <Card title="CSV Sample Rows">
                {selectedCsv ? (
                  sample.loading && !sample.data ? (
                    <Loading text="Loading CSV sample..." />
                  ) : sample.error ? (
                    <ErrorBlock error={sample.error} onRetry={sample.run} />
                  ) : sample.data?.rows?.length ? (
                    <table>
                      <thead>
                        <tr>
                          {(sample.data.headers || []).map((h) => (
                            <th key={h}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sample.data.rows.map((row, idx) => (
                          <tr key={idx}>
                            {(sample.data.headers || []).map((h) => (
                              <td key={`${idx}-${h}`} className="mono">
                                {row[h]}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <Empty text="CSV found, but no sample rows are available." />
                  )
                ) : (
                  <Empty text="Select a CSV file to see sample rows." />
                )}
              </Card>
            </>
          )
        ) : (
          <Empty text="No files for selected day." />
        )}
      </Card>
    </>
  );
}

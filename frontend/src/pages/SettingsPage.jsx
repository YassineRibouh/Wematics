import { useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading } from "../components/UiBits";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

export function SettingsPage() {
  const [variable, setVariable] = useState("RGB");
  const [description, setDescription] = useState("Visible spectrum image frames.");
  const [cadence, setCadence] = useState(15);
  const [isImageLike, setIsImageLike] = useState(true);

  const glossary = useAsync(() => api.getGlossary(), [], true);
  const runtime = useAsync(() => api.getOverview(), [], true);

  const saveGlossary = async () => {
    await api.upsertGlossary({
      variable,
      description,
      expected_cadence_seconds: cadence ? Number(cadence) : null,
      is_image_like: !!isImageLike,
    });
    glossary.run();
  };

  return (
    <>
      <div>
        <h2 className="page-title">Settings & Variable Glossary</h2>
        <p className="page-subtitle">Configure expected cadence and descriptive metadata per variable.</p>
      </div>

      <div className="grid two">
        <Card title="Add / Update Glossary Entry">
          <div className="controls">
            <label>
              Variable
              <input value={variable} onChange={(e) => setVariable(e.target.value)} />
            </label>
            <label>
              Expected cadence (sec)
              <input type="number" min={1} value={cadence} onChange={(e) => setCadence(e.target.value)} />
            </label>
            <label>
              Image-like
              <select value={String(isImageLike)} onChange={(e) => setIsImageLike(e.target.value === "true")}>
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            </label>
          </div>
          <label style={{ display: "grid", marginTop: "0.5rem" }}>
            Description
            <textarea rows={3} value={description} onChange={(e) => setDescription(e.target.value)} />
          </label>
          <div style={{ marginTop: "0.6rem" }}>
            <button onClick={saveGlossary}>Save</button>
          </div>
        </Card>

        <Card title="Runtime Info">
          {runtime.loading && !runtime.data ? (
            <Loading text="Loading..." />
          ) : runtime.error ? (
            <ErrorBlock error={runtime.error} onRetry={runtime.run} />
          ) : (
            <>
              <p className="mono">Last remote check: {runtime.data?.last_remote_check_time || "-"}</p>
              <p className="mono">Last remote file: {runtime.data?.last_new_file_time_remote || "-"}</p>
              <p className="mono">Last local file: {runtime.data?.last_new_file_time_local || "-"}</p>
              <p className="mono">Last ftp file: {runtime.data?.last_new_file_time_ftp || "-"}</p>
            </>
          )}
        </Card>
      </div>

      <Card title="Variable Glossary">
        {glossary.loading && !glossary.data ? (
          <Loading text="Loading glossary..." />
        ) : glossary.error ? (
          <ErrorBlock error={glossary.error} onRetry={glossary.run} />
        ) : glossary.data?.length ? (
          <table>
            <thead>
              <tr>
                <th>Variable</th>
                <th>Description</th>
                <th>Cadence</th>
                <th>Image-like</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {glossary.data.map((row) => (
                <tr key={row.variable}>
                  <td>{row.variable}</td>
                  <td>{row.description || "-"}</td>
                  <td>{row.expected_cadence_seconds || "-"}</td>
                  <td>{String(row.is_image_like)}</td>
                  <td className="mono">{row.updated_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <Empty text="No glossary entries yet." />
        )}
      </Card>
    </>
  );
}


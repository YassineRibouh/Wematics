import { useMemo, useState } from "react";
import { Card } from "../components/Card";
import { Empty, ErrorBlock, Loading, Notice } from "../components/UiBits";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

function midnightUtcSchedule(timezoneMode) {
  if (timezoneMode === "utc") {
    return { hour_of_day: 0, minute_of_hour: 0 };
  }
  const offsetMinutes = new Date().getTimezoneOffset();
  const utcTotal = ((offsetMinutes % (24 * 60)) + 24 * 60) % (24 * 60);
  return {
    hour_of_day: Math.floor(utcTotal / 60),
    minute_of_hour: utcTotal % 60,
  };
}

function nextUtcRunPreview(hour, minute) {
  const now = new Date();
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), hour, minute, 0));
  if (next <= now) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return {
    utc: next.toISOString(),
    local: next.toLocaleString(),
  };
}

export function AutomationPage() {
  const [name, setName] = useState("Sync today+yesterday");
  const [kind, setKind] = useState("download");
  const [everyMinutes, setEveryMinutes] = useState(15);
  const [camera, setCamera] = useState("ROS");
  const [variable, setVariable] = useState("RGB");
  const [presetTimezone, setPresetTimezone] = useState("local");
  const [verifyChecksum, setVerifyChecksum] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState({ tone: "info", text: "" });

  const schedules = useAsync(() => api.getSchedules(), [], true);
  const midnightConfig = useMemo(() => midnightUtcSchedule(presetTimezone), [presetTimezone]);
  const preview = useMemo(
    () => nextUtcRunPreview(midnightConfig.hour_of_day, midnightConfig.minute_of_hour),
    [midnightConfig.hour_of_day, midnightConfig.minute_of_hour]
  );

  const runAction = async (label, fn) => {
    setSaving(true);
    setMessage({ tone: "info", text: `${label}...` });
    try {
      await fn();
      setMessage({ tone: "success", text: `${label} completed.` });
    } catch (error) {
      setMessage({ tone: "error", text: `${label} failed: ${String(error?.message || error)}` });
    } finally {
      setSaving(false);
    }
  };

  const createSchedule = async () =>
    runAction("Creating schedule", async () => {
      await api.createSchedule({
        name,
        enabled: true,
        job_kind: kind,
        cadence: "interval",
        every_minutes: Number(everyMinutes),
        params: {
          camera,
          variable,
          mode: "rolling_days",
          rolling_days: 2,
          timezone: "local",
          csv_policy: "always_refresh",
          verify_checksum: verifyChecksum,
          run_isolated: false,
        },
      });
      await schedules.run();
    });

  const createTemplate = async (templateName) => {
    await runAction("Creating template", async () => {
      if (templateName === "daily7") {
        await api.createSchedule({
          name: `Daily verify last7 ${new Date().toISOString()}`,
          enabled: true,
          job_kind: "verify",
          cadence: "daily",
          hour_of_day: 2,
          minute_of_hour: 30,
          params: {
            source_a: "remote",
            source_b: "local",
            camera,
            variable,
            cadence_seconds: 15,
            date_from: null,
            date_to: null,
          },
        });
      }
      if (templateName === "weekly90") {
        await api.createSchedule({
          name: `Weekly verify last90 ${new Date().toISOString()}`,
          enabled: true,
          job_kind: "verify",
          cadence: "weekly",
          day_of_week: 0,
          hour_of_day: 3,
          minute_of_hour: 0,
          params: {
            source_a: "remote",
            source_b: "ftp",
            camera,
            variable,
            cadence_seconds: 15,
          },
        });
      }
      if (templateName === "midnightTransferDisabled") {
        await api.createSchedule({
          name: `Midnight transfer disabled ${camera}-${variable} ${new Date().toISOString()}`,
          enabled: false,
          job_kind: "transfer",
          cadence: "daily",
          hour_of_day: midnightConfig.hour_of_day,
          minute_of_hour: midnightConfig.minute_of_hour,
          params: {
            camera,
            variable,
            timezone: presetTimezone,
            mode: "rolling_days",
            rolling_days: 1,
            file_selection: "all",
            csv_policy: "always_refresh",
            verify_checksum: verifyChecksum,
            run_isolated: false,
            dry_run: false,
          },
        });
      }
      await schedules.run();
    });
  };

  const removeSchedule = async (id) =>
    runAction("Deleting schedule", async () => {
      await api.deleteSchedule(id);
      await schedules.run();
    });

  const toggleSchedule = async (row) =>
    runAction(`${row.enabled ? "Disabling" : "Enabling"} schedule`, async () => {
      await api.updateSchedule(row.id, {
        name: row.name,
        enabled: !row.enabled,
        job_kind: row.job_kind,
        cadence: row.cadence,
        every_minutes: row.every_minutes,
        hour_of_day: row.hour_of_day,
        minute_of_hour: row.minute_of_hour,
        day_of_week: row.day_of_week,
        day_of_month: row.day_of_month,
        params: row.params || {},
      });
      await schedules.run();
    });

  return (
    <>
      <div>
        <h2 className="page-title">Automation Schedules</h2>
        <p className="page-subtitle">Build interval and fixed-time automations, including disabled-by-default midnight transfer presets.</p>
      </div>

      <Card title="Create Schedule">
        <Notice tone={message.tone} text={message.text} />
        <div className="controls">
          <label>
            Name
            <input value={name} onChange={(e) => setName(e.target.value)} />
          </label>
          <label>
            Job kind
            <select value={kind} onChange={(e) => setKind(e.target.value)}>
              <option value="download">download</option>
              <option value="upload">upload</option>
              <option value="transfer">transfer</option>
              <option value="verify">verify</option>
              <option value="inventory_scan">inventory_scan</option>
            </select>
          </label>
          <label>
            Every minutes
            <input type="number" min={1} value={everyMinutes} onChange={(e) => setEveryMinutes(e.target.value)} />
          </label>
          <label>
            Camera
            <input value={camera} onChange={(e) => setCamera(e.target.value)} />
          </label>
          <label>
            Variable
            <input value={variable} onChange={(e) => setVariable(e.target.value)} />
          </label>
          <label>
            <input type="checkbox" checked={verifyChecksum} onChange={(e) => setVerifyChecksum(e.target.checked)} /> Verify checksum
          </label>
          <button disabled={saving} onClick={createSchedule}>
            Create
          </button>
          <button className="secondary" disabled={saving} onClick={() => createTemplate("daily7")}>
            Daily verify last 7 days
          </button>
          <button className="secondary" disabled={saving} onClick={() => createTemplate("weekly90")}>
            Weekly verify last 90 days
          </button>
        </div>
      </Card>

      <Card title="Midnight Transfer Preset">
        <div className="controls">
          <label>
            Data timezone
            <select value={presetTimezone} onChange={(e) => setPresetTimezone(e.target.value)}>
              <option value="local">local</option>
              <option value="utc">utc</option>
            </select>
          </label>
          <button className="secondary" disabled={saving} onClick={() => createTemplate("midnightTransferDisabled")}>
            Midnight transfer (disabled)
          </button>
        </div>
        <p className="mono">
          Preview next run: {preview.local} (local) | {preview.utc} (UTC)
        </p>
        <p className="mono">
          Scheduler UTC target: {String(midnightConfig.hour_of_day).padStart(2, "0")}:{String(midnightConfig.minute_of_hour).padStart(2, "0")}
        </p>
      </Card>

      {schedules.loading && !schedules.data ? <Loading text="Loading schedules..." /> : null}
      {schedules.error ? <ErrorBlock error={schedules.error} onRetry={schedules.run} /> : null}

      <Card title="Existing Schedules">
        {schedules.data?.length ? (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Kind</th>
                <th>Cadence</th>
                <th>Next run</th>
                <th>Last run</th>
                <th>Enabled</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {schedules.data.map((row) => (
                <tr key={row.id}>
                  <td>{row.name}</td>
                  <td>{row.job_kind}</td>
                  <td>{row.cadence}</td>
                  <td className="mono">{row.next_run_at || "-"}</td>
                  <td className="mono">{row.last_run_at || "-"}</td>
                  <td>
                    <span className={`tag ${row.enabled ? "ok" : ""}`}>{row.enabled ? "enabled" : "disabled"}</span>
                  </td>
                  <td>
                    <button className="secondary" disabled={saving} onClick={() => toggleSchedule(row)}>
                      {row.enabled ? "Disable" : "Enable"}
                    </button>
                    <button className="danger" disabled={saving} onClick={() => removeSchedule(row.id)} style={{ marginLeft: "0.4rem" }}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <Empty text="No schedules configured." />
        )}
      </Card>
    </>
  );
}

import { useEffect } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Card, Stat } from "../components/Card";
import { ErrorBlock, Loading } from "../components/UiBits";
import { api } from "../lib/api";
import { useAsync } from "../lib/useAsync";

export function OverviewPage() {
  const overview = useAsync(() => api.getOverview(), [], true);
  const jobs = useAsync(() => api.listJobs(), [], true);

  useEffect(() => {
    const id = setInterval(() => {
      overview.run().catch(() => {});
      jobs.run().catch(() => {});
    }, 15000);
    return () => clearInterval(id);
  }, [overview.run, jobs.run]);

  if (overview.loading && !overview.data) {
    return <Loading text="Loading dashboard metrics..." />;
  }
  if (overview.error) {
    return <ErrorBlock error={overview.error} onRetry={overview.run} />;
  }
  const data = overview.data || {};
  const counts = data.counts || {};
  const alerts = data.alerts || {};
  const jobsData = jobs.data || [];

  const chartData = [
    { name: "Local", value: counts.local_files || 0 },
    { name: "FTP", value: counts.ftp_files || 0 },
    { name: "Backlog", value: counts.ftp_backlog || 0 },
  ];

  return (
    <>
      <div>
        <h2 className="page-title">Overview</h2>
        <p className="page-subtitle">Health, freshness alerts, and queue activity.</p>
      </div>

      <div className="grid three">
        <Stat label="Running Jobs" value={data.job_counts?.running ?? 0} />
        <Stat label="Queued Jobs" value={data.job_counts?.queued ?? 0} />
        <Stat label="FTP Backlog" value={counts.ftp_backlog ?? 0} tone={counts.ftp_backlog > 0 ? "danger" : "ok"} />
      </div>

      <div className="grid two">
        <Card title="Alerts">
          <p>
            New-data status:{" "}
            <span className={`tag ${alerts.no_new_data?.status === "alert" ? "alert" : "ok"}`}>{alerts.no_new_data?.status}</span>
          </p>
          <p className="mono">Minutes since latest: {alerts.no_new_data?.minutes_since_latest ?? "-"}</p>
          <p>
            FTP backlog status:{" "}
            <span className={`tag ${alerts.ftp_backlog?.status === "alert" ? "alert" : "ok"}`}>{alerts.ftp_backlog?.status}</span>
          </p>
          <p className="mono">
            Count {alerts.ftp_backlog?.count ?? 0} / Threshold {alerts.ftp_backlog?.threshold ?? 0}
          </p>
          <p className="mono">Last remote check: {data.last_remote_check_time || "-"}</p>
        </Card>

        <Card title="Archive Counts">
          <div style={{ width: "100%", height: 230 }}>
            <ResponsiveContainer>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="value" fill="#005f8f" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      </div>

      <Card title="Recent Jobs" actions={<button onClick={() => jobs.run()}>Refresh</button>}>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Kind</th>
              <th>Status</th>
              <th>Created</th>
              <th>Started</th>
              <th>Ended</th>
            </tr>
          </thead>
          <tbody>
            {jobsData.slice(0, 15).map((job) => (
              <tr key={job.id}>
                <td className="mono">{job.id.slice(0, 8)}</td>
                <td>{job.kind}</td>
                <td>{job.status}</td>
                <td className="mono">{job.created_at}</td>
                <td className="mono">{job.started_at || "-"}</td>
                <td className="mono">{job.ended_at || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </>
  );
}


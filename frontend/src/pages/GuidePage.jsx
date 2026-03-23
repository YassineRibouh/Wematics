import { Link } from "react-router-dom";
import { Card } from "../components/Card";

const FLOW_STEPS = [
  {
    title: "Check Incoming Data",
    summary:
      "Start in Remote Explorer to confirm the source data is arriving for the camera, variable, and day you care about.",
    route: "/remote",
    routeLabel: "Open Remote Explorer",
  },
  {
    title: "Store a Local Copy",
    summary:
      "Use Transfer Center to run a download or transfer job. Local Explorer then shows what is already saved on this machine.",
    route: "/jobs",
    routeLabel: "Open Transfer Center",
  },
  {
    title: "Send to FTP",
    summary:
      "Upload or transfer jobs place files on the FTP server. FTP Explorer lets you inspect tracked uploads or browse server folders directly.",
    route: "/ftp",
    routeLabel: "Open FTP Explorer",
  },
  {
    title: "Verify Completeness",
    summary:
      "Diff & Gaps compares sources to catch missing days or partial days. Logs & Audit shows exactly what happened and when.",
    route: "/diff",
    routeLabel: "Open Diff & Gaps",
  },
];

const QUICK_START = [
  {
    title: "Pick your scope",
    text: "In Transfer Center, select camera and variable first. Keep timezone as local unless your team standard is UTC.",
  },
  {
    title: "Choose date range",
    text: "Use one day for a quick test. Move to multiple days only after the first run looks correct.",
  },
  {
    title: "Run safely",
    text: "Turn on Dry run to preview what would happen. Then run Transfer Job for full download + upload.",
  },
  {
    title: "Confirm results",
    text: "Review Local Explorer and FTP Explorer for expected files, then run Diff & Gaps or Verify job for confidence.",
  },
];

const PAGE_GUIDE = [
  {
    name: "Overview",
    route: "/",
    whatItMeans:
      "A health dashboard. It tells you if jobs are running, if FTP has backlog, and whether fresh data is still arriving.",
    useWhen: "Use first thing to spot issues fast before starting manual actions.",
    checkFor: "Alert tags, minutes since latest file, queued/running jobs, and recent job statuses.",
  },
  {
    name: "Remote Explorer",
    route: "/remote",
    whatItMeans:
      "A viewer for source data from Wematics. It can show image thumbnails by hour or inspect CSV files with a sample plot.",
    useWhen: "Use to validate source quality before any transfer.",
    checkFor: "Correct camera/variable, expected date coverage, and whether files are image-style or CSV-style.",
  },
  {
    name: "Local Explorer",
    route: "/local",
    whatItMeans:
      "Inventory of files stored locally. It includes scan, date summaries, file sizes, and per-day file listings.",
    useWhen: "Use after download/transfer to confirm local archive is complete.",
    checkFor: "Date rows, file counts, last modified, and scan status messages.",
  },
  {
    name: "FTP Explorer",
    route: "/ftp",
    whatItMeans:
      "Two views: tracked uploads (what jobs recorded) and raw FTP server browser (actual server folders/files).",
    useWhen: "Use after upload/transfer to verify destination state.",
    checkFor: "Expected date rows in tracked view and expected folders/files in server browser view.",
  },
  {
    name: "Diff & Gaps",
    route: "/diff",
    whatItMeans:
      "Comparison tool for two sources (remote/local/ftp). It highlights missing dates and partial coverage gaps.",
    useWhen: "Use for QA and confidence checks before reporting or handoff.",
    checkFor: "Missing dates list, partial day counts, and completeness percentages.",
  },
  {
    name: "Transfer Center",
    route: "/jobs",
    whatItMeans:
      "Main operations workflow. It creates download/upload/transfer/verify jobs with safe, resumable behavior.",
    useWhen: "Use for day-to-day execution and recovery after failures.",
    checkFor: "Current scope snapshot, queue statuses, and job events for any failed job.",
  },
  {
    name: "Automation",
    route: "/automation",
    whatItMeans:
      "Scheduler for recurring jobs (interval, daily, weekly). Includes templates and enabled/disabled controls.",
    useWhen: "Use when you want predictable recurring processing instead of manual runs.",
    checkFor: "Next run time, enabled flag, and schedule cadence.",
  },
  {
    name: "Settings",
    route: "/settings",
    whatItMeans:
      "Variable glossary and cadence metadata. Also shows runtime timing markers from the backend.",
    useWhen: "Use when terminology is unclear or expected cadence needs updating.",
    checkFor: "Descriptions, expected cadence seconds, and updated timestamps.",
  },
  {
    name: "Logs & Audit",
    route: "/logs",
    whatItMeans:
      "System history. Logs show process events; File Audit shows file-level actions and reasons.",
    useWhen: "Use whenever you need traceability or root-cause details.",
    checkFor: "Error levels, job IDs, file actions, and reason fields.",
  },
];

const GLOSSARY = [
  { term: "Camera", meaning: "The site or device source name (for example, ROS)." },
  { term: "Variable", meaning: "The data stream type (for example, RGB, temperature, etc.)." },
  { term: "Timezone", meaning: "How dates are interpreted in listings. Local follows your machine; UTC is universal." },
  { term: "Date range", meaning: "A fixed start and end date you choose manually." },
  { term: "Rolling days", meaning: "A moving window (for example last 7 days) relative to today." },
  { term: "Cadence", meaning: "Expected time spacing between files or measurements (for example every 15 seconds)." },
  { term: "Backlog", meaning: "Files waiting to be processed or uploaded." },
  { term: "Queued", meaning: "Job is waiting in line and has not started yet." },
  { term: "Running", meaning: "Job is currently processing files." },
  { term: "Completed", meaning: "Job finished without a failure." },
  { term: "Failed", meaning: "Job stopped due to an issue. Check Job Events or Logs for details." },
  { term: "Dry run", meaning: "Plan-only mode. Shows what would happen without moving files." },
  { term: "Checksum", meaning: "File integrity check to ensure source and destination copies match exactly." },
  { term: "Idempotent", meaning: "Same request can be retried safely without creating duplicate work." },
  { term: "Inventory scan", meaning: "Refreshes local file index so the app can show current archive state." },
  { term: "Verify job", meaning: "Checks source-vs-destination completeness and interval gaps." },
  { term: "Mirrored", meaning: "Date exists in both compared sources." },
  { term: "Partial day", meaning: "A day with some data missing in expected intervals." },
  { term: "Audit event", meaning: "Recorded file-level action and why it happened." },
  { term: "FTP server browser", meaning: "Direct folder view of FTP, including non-Wematics files." },
];

const TROUBLESHOOTING = [
  {
    title: "No files appear",
    text: "Confirm camera + variable first, then switch timezone to see if date boundaries change.",
  },
  {
    title: "Job failed",
    text: "Select the failed job in Transfer Center, review Job Events, then use Resume Selected Failed Job.",
  },
  {
    title: "FTP looks incomplete",
    text: "Use FTP Completeness Check in Transfer Center and then run Verify or Diff & Gaps to quantify gaps.",
  },
  {
    title: "Automation did not run",
    text: "Open Automation, verify schedule is enabled, and check Next run plus Logs for execution details.",
  },
];

export function GuidePage() {
  return (
    <>
      <section className="guide-hero">
        <p className="guide-kicker">Wematics ASI User Guide</p>
        <h2 className="page-title">How To Use The App (Non-Technical)</h2>
        <p className="page-subtitle">
          This page explains what every screen means, how data moves, and the safest daily routine.
        </p>
        <div className="guide-action-row">
          <Link className="link-button" to="/jobs">
            Start with Transfer Center
          </Link>
          <Link className="link-button secondary" to="/">
            Dashboard Overview
          </Link>
          <Link className="link-button secondary" to="/logs">
            View Logs
          </Link>
        </div>
      </section>

      <Card title="How It Works In Plain Language">
        <div className="guide-flow-grid">
          {FLOW_STEPS.map((step, idx) => (
            <article key={step.title} className="guide-flow-step">
              <span className="guide-step-index">{idx + 1}</span>
              <h3>{step.title}</h3>
              <p>{step.summary}</p>
              <Link className="link-button secondary" to={step.route}>
                {step.routeLabel}
              </Link>
            </article>
          ))}
        </div>
      </Card>

      <Card title="First-Day Checklist">
        <ol className="guide-checklist">
          {QUICK_START.map((item) => (
            <li key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.text}</p>
            </li>
          ))}
        </ol>
      </Card>

      <Card title="What Each Page Means">
        <div className="guide-page-grid">
          {PAGE_GUIDE.map((page) => (
            <article key={page.name} className="guide-page-card">
              <header>
                <h3>{page.name}</h3>
                <Link className="link-button secondary" to={page.route}>
                  Open
                </Link>
              </header>
              <p>
                <strong>What it is:</strong> {page.whatItMeans}
              </p>
              <p>
                <strong>When to use it:</strong> {page.useWhen}
              </p>
              <p>
                <strong>What to check:</strong> {page.checkFor}
              </p>
            </article>
          ))}
        </div>
      </Card>

      <div className="grid two">
        <Card title="Glossary (Common Terms)">
          <div className="guide-glossary-grid">
            {GLOSSARY.map((item) => (
              <article key={item.term} className="guide-term-card">
                <h3>{item.term}</h3>
                <p>{item.meaning}</p>
              </article>
            ))}
          </div>
        </Card>

        <Card title="If Something Goes Wrong">
          <div className="guide-trouble-grid">
            {TROUBLESHOOTING.map((item) => (
              <article key={item.title} className="guide-trouble-card">
                <h3>{item.title}</h3>
                <p>{item.text}</p>
              </article>
            ))}
          </div>
        </Card>
      </div>
    </>
  );
}

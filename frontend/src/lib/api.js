const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api";

function buildQuery(params = {}) {
  return new URLSearchParams(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== "")
  ).toString();
}

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  const type = response.headers.get("content-type") || "";
  if (type.includes("application/json")) {
    return response.json();
  }
  return response.text();
}

export const api = {
  getOverview: () => request("/overview"),
  getRemoteCameras: () => request("/remote/cameras"),
  getRemoteVariables: (camera) => request(`/remote/variables/${encodeURIComponent(camera)}`),
  getRemoteDates: (camera, variable, timezone = "local") =>
    request(`/remote/dates?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&timezone=${timezone}`),
  getRemoteFiles: (camera, variable, date, timezone = "local") =>
    request(
      `/remote/files?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(date)}&timezone=${timezone}`
    ),
  getRemoteFileSample: (camera, variable, date, filename, timezone = "local", rows = 10) =>
    request(
      `/remote/file-sample?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(
        date
      )}&filename=${encodeURIComponent(filename)}&timezone=${timezone}&rows=${rows}`
    ),
  getRemoteFileAnalysis: (
    camera,
    variable,
    date,
    filename,
    timezone = "local",
    rows = 3000,
    timeColumn = "",
    valueColumn = ""
  ) =>
    request(
      `/remote/file-analysis?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(
        date
      )}&filename=${encodeURIComponent(filename)}&timezone=${timezone}&rows=${rows}&time_column=${encodeURIComponent(
        timeColumn
      )}&value_column=${encodeURIComponent(valueColumn)}`
    ),
  triggerLocalScan: (camera, variable) =>
    request(`/local/scan?camera=${encodeURIComponent(camera || "")}&variable=${encodeURIComponent(variable || "")}`, {
      method: "POST",
    }),
  getLocalDates: (camera, variable) =>
    request(`/local/dates?camera=${encodeURIComponent(camera || "")}&variable=${encodeURIComponent(variable || "")}`),
  getLocalFiles: ({ camera, variable, date, page = 1, pageSize = 200, search = "" }) =>
    request(
      `/local/files?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(
        date
      )}&page=${page}&page_size=${pageSize}&search=${encodeURIComponent(search)}`
    ),
  getStorageSummary: (camera, variable) =>
    request(`/local/storage-summary?camera=${encodeURIComponent(camera || "")}&variable=${encodeURIComponent(variable || "")}`),
  getFtpDates: (camera, variable) =>
    request(`/ftp/dates?camera=${encodeURIComponent(camera || "")}&variable=${encodeURIComponent(variable || "")}`),
  getFtpFiles: ({ camera, variable, date, page = 1, pageSize = 200, search = "" }) =>
    request(
      `/ftp/files?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(
        date
      )}&page=${page}&page_size=${pageSize}&search=${encodeURIComponent(search)}`
    ),
  getFtpServerList: (path = "/", limit = 2000) =>
    request(`/ftp/server/list?path=${encodeURIComponent(path)}&limit=${limit}`),
  getFtpServerDownloadUrl: (path) => `${API_BASE}/ftp/server/download?path=${encodeURIComponent(path)}`,
  computeDiff: (payload) =>
    request("/diff/compute", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  listJobs: () => request("/jobs"),
  getJobFtpAvailability: ({ camera, variable, timezone = "local", dateFrom = "", dateTo = "", maxDays = 31 }) =>
    request(
      `/jobs/ftp-availability?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(
        variable
      )}&timezone=${encodeURIComponent(timezone)}&date_from=${encodeURIComponent(dateFrom)}&date_to=${encodeURIComponent(
        dateTo
      )}&max_days=${maxDays}`
    ),
  resumeJob: (jobId, { failedOnly = true } = {}) =>
    request(`/jobs/${encodeURIComponent(jobId)}/resume?failed_only=${failedOnly ? "true" : "false"}`, {
      method: "POST",
    }),
  cancelJob: (jobId) =>
    request(`/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
    }),
  getJobEvents: (jobId) => request(`/jobs/${encodeURIComponent(jobId)}/events`),
  getJobFailures: (jobId, limit = 50) => request(`/jobs/${encodeURIComponent(jobId)}/failures?limit=${limit}`),
  createDownloadJob: (payload) =>
    request("/jobs/download", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createUploadJob: (payload) =>
    request("/jobs/upload", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createTransferJob: (payload) =>
    request("/jobs/transfer", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createVerifyJob: (payload) =>
    request("/jobs/verify", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createInventoryJob: (camera, variable) =>
    request(`/jobs/inventory?camera=${encodeURIComponent(camera || "")}&variable=${encodeURIComponent(variable || "")}`, {
      method: "POST",
    }),
  getSchedules: () => request("/schedules"),
  createSchedule: (payload) =>
    request("/schedules", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  updateSchedule: (id, payload) =>
    request(`/schedules/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    }),
  deleteSchedule: (id) => request(`/schedules/${encodeURIComponent(id)}`, { method: "DELETE" }),
  getGlossary: () => request("/glossary"),
  upsertGlossary: (payload) =>
    request("/glossary", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getJob: (jobId) => request(`/jobs/${encodeURIComponent(jobId)}`),
  getFileLineage: ({ camera, variable, date, filename }) =>
    request(
      `/files/lineage?camera=${encodeURIComponent(camera)}&variable=${encodeURIComponent(variable)}&date=${encodeURIComponent(
        date
      )}&filename=${encodeURIComponent(filename)}`
    ),
  getLogs: (params = {}) => {
    const query = buildQuery(params);
    return request(`/logs${query ? `?${query}` : ""}`);
  },
  getAudit: (params = {}) => {
    const query = buildQuery(params);
    return request(`/audit${query ? `?${query}` : ""}`);
  },
  searchFiles: (params = {}) => {
    const query = buildQuery(params);
    return request(`/files/search${query ? `?${query}` : ""}`);
  },
};


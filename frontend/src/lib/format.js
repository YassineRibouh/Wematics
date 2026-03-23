const IMAGE_EXTENSIONS = /\.(webp|jpg|jpeg|png)$/i;

export function formatBytes(value) {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let n = Number(value);
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatNumber(value) {
  const parsed = Number(value);
  if (value === null || value === undefined || Number.isNaN(parsed)) return "-";
  return parsed.toLocaleString();
}

export function isImageFilename(name) {
  return IMAGE_EXTENSIONS.test(String(name || ""));
}

export function isCsvFilename(name) {
  return String(name || "").toLowerCase().endsWith(".csv");
}

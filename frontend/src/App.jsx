import { Suspense, lazy } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { Loading } from "./components/UiBits";

const OverviewPage = lazy(() => import("./pages/OverviewPage").then((m) => ({ default: m.OverviewPage })));
const SearchPage = lazy(() => import("./pages/SearchPage").then((m) => ({ default: m.SearchPage })));
const RemoteExplorerPage = lazy(() => import("./pages/RemoteExplorerPage").then((m) => ({ default: m.RemoteExplorerPage })));
const LocalExplorerPage = lazy(() => import("./pages/LocalExplorerPage").then((m) => ({ default: m.LocalExplorerPage })));
const FtpExplorerPage = lazy(() => import("./pages/FtpExplorerPage").then((m) => ({ default: m.FtpExplorerPage })));
const DiffPage = lazy(() => import("./pages/DiffPage").then((m) => ({ default: m.DiffPage })));
const JobsPage = lazy(() => import("./pages/JobsPage").then((m) => ({ default: m.JobsPage })));
const AutomationPage = lazy(() => import("./pages/AutomationPage").then((m) => ({ default: m.AutomationPage })));
const SettingsPage = lazy(() => import("./pages/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const LogsPage = lazy(() => import("./pages/LogsPage").then((m) => ({ default: m.LogsPage })));
const GuidePage = lazy(() => import("./pages/GuidePage").then((m) => ({ default: m.GuidePage })));

export default function App() {
  return (
    <Layout>
      <Suspense fallback={<Loading text="Loading page..." />}>
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/remote" element={<RemoteExplorerPage />} />
          <Route path="/local" element={<LocalExplorerPage />} />
          <Route path="/ftp" element={<FtpExplorerPage />} />
          <Route path="/diff" element={<DiffPage />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/automation" element={<AutomationPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/guide" element={<GuidePage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Suspense>
    </Layout>
  );
}

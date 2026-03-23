import { NavLink } from "react-router-dom";

const NAV = [
  { to: "/", label: "Overview" },
  { to: "/search", label: "Search" },
  { to: "/remote", label: "Remote Explorer" },
  { to: "/local", label: "Local Explorer" },
  { to: "/diff", label: "Diff & Gaps" },
  { to: "/jobs", label: "Transfer Builder" },
  { to: "/ftp", label: "FTP Explorer" },
  { to: "/automation", label: "Automation" },
  { to: "/settings", label: "Settings" },
  { to: "/logs", label: "Logs & Audit" },
  { to: "/guide", label: "Guide" },
];

export function Layout({ children }) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <span className="sidebar-kicker">Archive Console</span>
          <h1>Wematics ASI</h1>
          <p>Remote capture, fast transfer to FTP, and the core audit trail in one simpler workflow.</p>
        </div>

        <nav className="sidebar-nav">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="sidebar-footer">
          <p className="mono">Ops focus</p>
          <p>Pick the scope, run the transfer, and watch the result.</p>
        </div>
      </aside>
      <main className="content">
        <div className="content-inner">{children}</div>
      </main>
    </div>
  );
}

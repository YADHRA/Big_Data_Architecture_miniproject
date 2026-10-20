import { NavLink, Outlet } from 'react-router-dom'
import {
  Activity,
  AlertTriangle,
  BarChart3,
  LayoutDashboard,
  Users,
  Database,
} from 'lucide-react'

const navigation = [
  {
    label: 'Dashboard',
    path: '/dashboard',
    icon: LayoutDashboard,
  },
  {
    label: 'Usage Analytics',
    path: '/usage',
    icon: BarChart3,
  },
  {
    label: 'Anomalies',
    path: '/anomalies',
    icon: AlertTriangle,
  },
  {
    label: 'Segments',
    path: '/segments',
    icon: Users,
  },
]

const insightNavigation = [
  {
    label: 'Power BI',
    path: '/powerbi',
    icon: Database,
  },
]

function AppLayout() {
  return (
    <div className="app-shell">

      {/* =========================
          SIDEBAR
      ========================== */}
      <aside className="sidebar">

        {/* Brand */}
        <div className="brand">
          <div className="brand-mark">
            <Activity size={20} strokeWidth={2.2} />
          </div>

          <div>
            <div className="brand-name">
              Internet Analytics
            </div>

            <div className="brand-subtitle">
              Intelligence Platform
            </div>
          </div>
        </div>

        {/* Analytics navigation */}
        <div className="sidebar-section">

          <div className="sidebar-section-label">
            Analytics
          </div>

          <nav className="nav-list">
            {navigation.map((item) => {
              const Icon = item.icon

              return (
                <NavLink
                  key={item.path}
                  to={item.path}
                  className={({ isActive }) =>
                    `nav-item ${isActive ? 'active' : ''}`
                  }
                >
                  <span className="nav-icon">
                    <Icon size={17} strokeWidth={1.8} />
                  </span>

                  <span>
                    {item.label}
                  </span>
                </NavLink>
              )
            })}
          </nav>
        </div>

        {/* Insights navigation */}
        <div className="sidebar-section">

          <div className="sidebar-section-label">
            Insights
          </div>

          <nav className="nav-list">
            {insightNavigation.map((item) => {
              const Icon = item.icon

              return (
                <NavLink
                  key={item.path}
                  to={item.path}
                  className={({ isActive }) =>
                    `nav-item ${isActive ? 'active' : ''}`
                  }
                >
                  <span className="nav-icon">
                    <Icon size={17} strokeWidth={1.8} />
                  </span>

                  <span>
                    {item.label}
                  </span>
                </NavLink>
              )
            })}
          </nav>
        </div>

        {/* Connection status */}
        <div className="sidebar-bottom">
          <div className="connection-status">
            <span className="status-dot" />
            <span>Backend Connected</span>
          </div>
        </div>

      </aside>

      {/* =========================
          MAIN CONTENT
      ========================== */}
      <main className="main-content">

        {/* Top bar */}
        <header className="topbar">

          <div>
            <div className="topbar-eyebrow">
              Analytics Platform
            </div>

            <div className="topbar-title">
              Internet Usage Intelligence
            </div>
          </div>

          <div className="system-status">
            <span className="status-dot" />
            <span>System Online</span>
          </div>

        </header>

        {/* Page */}
        <section className="page-content">
          <Outlet />
        </section>

      </main>

    </div>
  )
}

export default AppLayout

import { useEffect, useState } from 'react'
import {
  Activity,
  Database,
  Users,
  AlertTriangle,
  RefreshCw,
} from 'lucide-react'
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

import {
  getKPIs,
  getUsageByApps,
  getUsageByCategories,
  getUsageByOperators,
  getUsageHourly,
  getAnomalies,
  getAnomalyThresholds,
  getSessionClusters,
  getCustomerClusters,
} from '../services/api'

const number = (value, digits = 0) =>
  Number(value ?? 0).toLocaleString('en-US', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  })

const asArray = (value) => {
  if (Array.isArray(value)) return value
  if (Array.isArray(value?.items)) return value.items
  if (Array.isArray(value?.data)) return value.data
  return []
}

const cardStyle = {
  background: '#0c1118',
  border: '1px solid rgba(255,255,255,0.08)',
  borderRadius: 14,
  padding: 20,
  minWidth: 0,
}

const tooltipStyle = {
  background: '#111827',
  border: '1px solid #334155',
  borderRadius: 8,
  color: '#f8fafc',
}

function MetricCard({ label, value, helper, icon: Icon }) {
  return (
    <article style={cardStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
        <span style={{ color: '#94a3b8', fontSize: 13 }}>{label}</span>
        <Icon size={19} color="#8da2ff" />
      </div>
      <div style={{ fontSize: 27, fontWeight: 700, marginTop: 14 }}>
        {value}
      </div>
      <div style={{ color: '#8190a5', fontSize: 12, marginTop: 8 }}>
        {helper}
      </div>
    </article>
  )
}

function ChartCard({ title, description, children }) {
  return (
    <section style={cardStyle}>
      <h2 style={{ fontSize: 16, margin: '0 0 7px' }}>{title}</h2>
      <p style={{ color: '#8190a5', fontSize: 12, margin: '0 0 18px' }}>
        {description}
      </p>
      {children}
    </section>
  )
}

function EmptyChart({ message = 'No data returned by the API.' }) {
  return (
    <div style={{ height: 260, display: 'grid', placeItems: 'center', color: '#8190a5', fontSize: 13 }}>
      {message}
    </div>
  )
}

function PowerBI() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  async function loadDashboard() {
    setLoading(true)
    setError('')

    const requests = await Promise.allSettled([
      getKPIs(),
      getUsageByApps(),
      getUsageByCategories(),
      getUsageByOperators(),
      getUsageHourly(),
      getAnomalies(),
      getAnomalyThresholds(),
      getSessionClusters(),
      getCustomerClusters(),
    ])

    const failures = requests.filter((item) => item.status === 'rejected')
    const values = requests.map((item) =>
      item.status === 'fulfilled' ? item.value : null,
    )

    if (!values[0]) {
      setError(
        'The KPI API could not be loaded. Check that the FastAPI backend is running on port 8000.',
      )
      setLoading(false)
      return
    }

    setData({
      kpis: values[0],
      apps: asArray(values[1]),
      categories: asArray(values[2]),
      operators: asArray(values[3]),
      hourly: asArray(values[4]),
      anomalies: asArray(values[5]),
      thresholds: asArray(values[6]),
      sessionClusters: asArray(values[7]),
      customerClusters: asArray(values[8]),
    })

    if (failures.length) {
      setError(
        `${failures.length} of 9 API requests failed. Some charts may be empty; check the backend endpoints.`,
      )
    }

    setLoading(false)
  }

  useEffect(() => {
    loadDashboard()
  }, [])

  const panelTitle = {
    fontSize: 28,
    margin: '0 0 8px',
    fontWeight: 750,
    letterSpacing: '-0.6px',
  }

  if (loading) {
    return (
      <main style={{ color: '#edf2f7', padding: 24 }}>
        <p style={{ color: '#94a3b8' }}>Loading verified analytics from FastAPI…</p>
      </main>
    )
  }

  if (!data) {
    return (
      <main style={{ color: '#edf2f7', padding: 24 }}>
        <h1 style={panelTitle}>Power BI Analytics</h1>
        <p style={{ color: '#f2ad68' }}>{error}</p>
        <button onClick={loadDashboard}>Retry</button>
      </main>
    )
  }

  const { kpis } = data

  return (
    <main style={{ color: '#edf2f7', display: 'grid', gap: 22, padding: '24px 0' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
        <div>
          <div style={{ color: '#8da2ff', fontSize: 11, fontWeight: 700, letterSpacing: 2 }}>
            REPORTING & BUSINESS INTELLIGENCE
          </div>
          <h1 style={panelTitle}>Internet Usage Analytics</h1>
          <p style={{ color: '#94a3b8', margin: 0 }}>
            Interactive report powered by the project's existing FastAPI analytics.
          </p>
        </div>
        <button
          onClick={loadDashboard}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '10px 14px',
            borderRadius: 9,
            border: '1px solid #334155',
            background: '#101720',
            color: '#edf2f7',
            cursor: 'pointer',
          }}
        >
          <RefreshCw size={15} /> Refresh data
        </button>
      </header>

      {error && (
        <div role="status" style={{ ...cardStyle, color: '#f2ad68' }}>
          {error}
        </div>
      )}

      <div style={{ color: '#94a3b8', fontSize: 12 }}>
        Source: existing project API and CSV exports. This is a React analytics
        report, not an embedded Power BI Service report.
      </div>

      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 14 }}>
        <MetricCard
          label="Total sessions"
          value={number(kpis.total_sessions)}
          helper="All dataset sessions"
          icon={Activity}
        />
        <MetricCard
          label="Unique customers"
          value={number(kpis.unique_customers)}
          helper="Overall dataset count"
          icon={Users}
        />
        <MetricCard
          label="Total data"
          value={`${number(Number(kpis.total_data_mb) / 1024, 1)} GB`}
          helper={`${number(kpis.total_data_mb, 2)} MB`}
          icon={Database}
        />
        <MetricCard
          label="Flagged usage anomalies"
          value={number(kpis.total_anomalies)}
          helper={`${number(kpis.anomaly_pct, 4)}% of sessions`}
          icon={AlertTriangle}
        />
      </section>

      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 16 }}>
        <ChartCard
          title="Data consumption by application"
          description="Total data consumed by app, in MB."
        >
          {data.apps.length ? (
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={data.apps} margin={{ top: 8, right: 8, left: 0, bottom: 35 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="app_name" tick={{ fill: '#94a3b8', fontSize: 10 }} angle={-25} textAnchor="end" interval={0} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [`${number(value, 2)} MB`, 'Total data']} />
                <Bar dataKey="total_data_mb" fill="#8195ff" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>

        <ChartCard
          title="Usage by category"
          description="Compare the total sessions across app categories."
        >
          {data.categories.length ? (
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={data.categories} margin={{ top: 8, right: 8, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="app_category" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [number(value), 'Sessions']} />
                <Bar dataKey="sessions" fill="#50d890" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>

        <ChartCard
          title="Usage by operator"
          description="Session counts by telecom operator."
        >
          {data.operators.length ? (
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={data.operators} margin={{ top: 8, right: 8, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="operator" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [number(value), 'Sessions']} />
                <Bar dataKey="sessions" fill="#c084fc" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>

        <ChartCard
          title="Hourly usage pattern"
          description="Session volume by hour of day."
        >
          {data.hourly.length ? (
            <ResponsiveContainer width="100%" height={290}>
              <LineChart data={[...data.hourly].sort((a, b) => a.hour_of_day - b.hour_of_day)} margin={{ top: 8, right: 8, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" />
                <XAxis dataKey="hour_of_day" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [number(value), 'Sessions']} />
                <Line type="monotone" dataKey="sessions" stroke="#f2ad68" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>
      </section>

      <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 16 }}>
        <ChartCard
          title="Category-specific anomaly thresholds"
          description="Upper consumption bounds used by the IQR method."
        >
          {data.thresholds.length ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={data.thresholds} margin={{ top: 8, right: 8, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="app_category" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [`${number(value, 3)} MB`, 'Upper bound']} />
                <Bar dataKey="anomaly_threshold_mb" fill="#f27685" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>

        <ChartCard
          title="Session cluster distribution"
          description="Sessions assigned to each K-Means cluster."
        >
          {data.sessionClusters.length ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={data.sessionClusters} margin={{ top: 8, right: 8, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="session_cluster" tick={{ fill: '#94a3b8', fontSize: 11 }} tickFormatter={(v) => `C${v}`} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <Tooltip contentStyle={tooltipStyle} formatter={(value) => [number(value), 'Sessions']} />
                <Bar dataKey="sessions" fill="#5cc8d7" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <EmptyChart />}
        </ChartCard>
      </section>

      <section style={cardStyle}>
        <h2 style={{ margin: '0 0 6px', fontSize: 16 }}>Customer segments</h2>
        <p style={{ margin: '0 0 16px', color: '#8190a5', fontSize: 12 }}>
          Profiles for customers with at least two sessions.
        </p>
        {data.customerClusters.length ? (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 13, textAlign: 'left' }}>
              <thead>
                <tr style={{ color: '#94a3b8', borderBottom: '1px solid #253041' }}>
                  <th style={{ padding: 12 }}>Segment</th>
                  <th style={{ padding: 12 }}>Customers</th>
                  <th style={{ padding: 12 }}>Avg data/session</th>
                  <th style={{ padding: 12 }}>Avg duration</th>
                  <th style={{ padding: 12 }}>Share</th>
                </tr>
              </thead>
              <tbody>
                {data.customerClusters.map((cluster) => (
                  <tr key={cluster.cluster} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={{ padding: 12 }}>{cluster.cluster_name || `Cluster ${cluster.cluster}`}</td>
                    <td style={{ padding: 12 }}>{number(cluster.customer_count)}</td>
                    <td style={{ padding: 12 }}>{number(cluster.avg_data_mb, 2)} MB</td>
                    <td style={{ padding: 12 }}>{number(cluster.avg_session_duration_min, 2)} min</td>
                    <td style={{ padding: 12 }}>{number(cluster.customer_share_pct, 2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : <EmptyChart />}
      </section>

      <section style={{ ...cardStyle, color: '#94a3b8', fontSize: 12, lineHeight: 1.7 }}>
        <strong style={{ color: '#edf2f7' }}>Interpretation and limitations</strong>
        <p>
          This project uses a synthetic telecom usage dataset. Anomaly flags indicate
          consumption above category-specific IQR thresholds; they do not establish
          fraud. Session clusters and repeat-customer clusters describe different
          populations and should be interpreted separately.
        </p>
      </section>
    </main>
  )
}

export default PowerBI

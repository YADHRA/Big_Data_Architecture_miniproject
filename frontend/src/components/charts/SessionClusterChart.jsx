import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

const clusterNames = {
  0: 'Long Social Sessions',
  1: 'High-Data Streaming Sessions',
  2: 'Short Social Sessions',
}

function SessionClusterChart({ data = [] }) {
  const chartData = data.map((item) => ({
    name: `C${item.session_cluster}`,
    cluster: clusterNames[item.session_cluster] || `Cluster ${item.session_cluster}`,
    sessions: Number(item.sessions),
  }))

  return (
    <div className="segment-chart-card">
      <div className="segment-chart-header">
        <div>
          <div className="chart-eyebrow">SESSION SEGMENTATION</div>
          <h2 className="chart-title">Session Cluster Distribution</h2>
          <p className="chart-description">
            Distribution of usage sessions across the three K-Means clusters.
          </p>
        </div>
      </div>

      <div className="segment-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No session cluster data available.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              data={chartData}
              margin={{
                top: 10,
                right: 20,
                left: 10,
                bottom: 10,
              }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="rgba(148, 163, 184, 0.08)"
                vertical={false}
              />

              <XAxis
                dataKey="name"
                tick={{
                  fill: '#94a3b8',
                  fontSize: 11,
                }}
                tickLine={false}
                axisLine={false}
              />

              <YAxis
                tick={{
                  fill: '#64748b',
                  fontSize: 11,
                }}
                tickLine={false}
                axisLine={false}
              />

              <Tooltip
                formatter={(value) => [
                  Number(value).toLocaleString('en-US'),
                  'Sessions',
                ]}
                labelFormatter={(label) => {
                  const item = chartData.find(
                    (entry) => entry.name === label,
                  )

                  return item
                    ? `${label} — ${item.cluster}`
                    : label
                }}
                contentStyle={{
                  background: '#111827',
                  border: '1px solid rgba(148, 163, 184, 0.18)',
                  borderRadius: '12px',
                  color: '#f8fafc',
                }}
              />

              <Bar
                dataKey="sessions"
                name="Sessions"
                fill="#818cf8"
                radius={[6, 6, 0, 0]}
                maxBarSize={70}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default SessionClusterChart
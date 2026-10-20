import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

const CATEGORIES = [
  'other',
  'social',
  'streaming',
]

function AnomalyCategoryChart({ data = [] }) {
  const counts = data.reduce((acc, item) => {
    const category = item.app_category || 'unknown'

    acc[category] = (acc[category] || 0) + 1

    return acc
  }, {})

  const chartData = CATEGORIES.map((category) => ({
    category,
    anomalies: counts[category] || 0,
  }))

  return (
    <div className="anomaly-chart-card">
      <div className="anomaly-chart-header">
        <div>
          <div className="chart-eyebrow">
            ANOMALY DISTRIBUTION
          </div>

          <h2 className="chart-title">
            Usage Anomalies by Category
          </h2>

          <p className="chart-description">
            Number of sessions exceeding their
            category-specific usage threshold.
          </p>
        </div>
      </div>

      <div className="anomaly-chart-container">
        {data.length === 0 ? (
          <div className="chart-empty">
            No anomaly data available.
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
                dataKey="category"
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
                  'Anomalies',
                ]}
                contentStyle={{
                  background: '#111827',
                  border:
                    '1px solid rgba(148, 163, 184, 0.18)',
                  borderRadius: '12px',
                  color: '#f8fafc',
                }}
              />

              <Bar
                dataKey="anomalies"
                name="Anomalies"
                fill="#f97316"
                radius={[6, 6, 0, 0]}
                maxBarSize={65}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default AnomalyCategoryChart
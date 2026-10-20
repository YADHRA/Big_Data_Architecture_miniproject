import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

function AnomalyThresholdChart({ data = [] }) {
  const chartData = [...data]
    .sort(
      (a, b) =>
        a.anomaly_threshold_mb -
        b.anomaly_threshold_mb,
    )
    .map((item) => ({
      category: item.app_category,
      threshold: Number(
        item.anomaly_threshold_mb,
      ),
    }))

  return (
    <div className="anomaly-chart-card">
      <div className="anomaly-chart-header">
        <div>
          <div className="chart-eyebrow">
            IQR THRESHOLDS
          </div>

          <h2 className="chart-title">
            Category Usage Thresholds
          </h2>

          <p className="chart-description">
            Upper usage bounds calculated independently
            for each application category.
          </p>
        </div>
      </div>

      <div className="anomaly-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No threshold data available.
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
                tickFormatter={(value) => `${value} MB`}
              />

              <Tooltip
                formatter={(value) => [
                  `${Number(value).toLocaleString(
                    'en-US',
                    {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    },
                  )} MB`,
                  'Upper bound',
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
                dataKey="threshold"
                name="Upper bound"
                fill="#818cf8"
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

export default AnomalyThresholdChart
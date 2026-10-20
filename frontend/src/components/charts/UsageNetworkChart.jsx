import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
} from 'recharts'

const COLORS = [
  '#818cf8',
  '#22c55e',
  '#f59e0b',
  '#ef4444',
]

function UsageNetworkChart({ data = [] }) {
  const chartData = data.map((item) => ({
    name: item.network_type,
    value: Number(item.total_data_mb),
  }))

  return (
    <div className="usage-chart-card">
      <div className="usage-chart-header">
        <div>
          <div className="chart-eyebrow">
            NETWORK DISTRIBUTION
          </div>

          <h2 className="chart-title">
            Data Consumption by Network
          </h2>

          <p className="chart-description">
            Distribution of consumed data across network types.
          </p>
        </div>
      </div>

      <div className="usage-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No network usage data available.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={350}>
            <PieChart>
              <Pie
                data={chartData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="45%"
                innerRadius={75}
                outerRadius={115}
                paddingAngle={3}
                stroke="none"
              >
                {chartData.map((entry, index) => (
                  <Cell
                    key={`network-${entry.name}`}
                    fill={COLORS[index % COLORS.length]}
                  />
                ))}
              </Pie>

              <Tooltip
                formatter={(value) => [
                  `${Number(value / 1000).toLocaleString(
                    'en-US',
                    {
                      maximumFractionDigits: 2,
                    },
                  )} GB`,
                  'Data consumed',
                ]}
                contentStyle={{
                  background: '#111827',
                  border: '1px solid rgba(148, 163, 184, 0.18)',
                  borderRadius: '12px',
                  color: '#f8fafc',
                }}
              />

              <Legend
                verticalAlign="bottom"
                height={36}
                iconType="circle"
              />
            </PieChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default UsageNetworkChart
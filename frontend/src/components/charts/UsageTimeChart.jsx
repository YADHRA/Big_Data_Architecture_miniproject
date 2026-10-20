import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

const TIME_ORDER = [
  'morning',
  'afternoon',
  'evening',
  'night',
]

function UsageTimeChart({ data = [] }) {
  const chartData = [...data]
    .sort(
      (a, b) =>
        TIME_ORDER.indexOf(a.time_of_day) -
        TIME_ORDER.indexOf(b.time_of_day),
    )
    .map((item) => ({
      name: item.time_of_day,
      dataGB: Number(
        (item.total_data_mb / 1000).toFixed(2),
      ),
      sessions: Number(item.sessions),
    }))

  return (
    <div className="usage-chart-card">
      <div className="usage-chart-header">
        <div>
          <div className="chart-eyebrow">
            TIME-OF-DAY PATTERN
          </div>

          <h2 className="chart-title">
            Usage by Time of Day
          </h2>

          <p className="chart-description">
            Data consumption across morning, afternoon, evening and night.
          </p>
        </div>
      </div>

      <div className="usage-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No time-of-day data available.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={350}>
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
                tickFormatter={(value) =>
                  value.charAt(0).toUpperCase() +
                  value.slice(1)
                }
              />

              <YAxis
                tick={{
                  fill: '#64748b',
                  fontSize: 11,
                }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) => `${value} GB`}
              />

              <Tooltip
                formatter={(value, name) => {
                  if (name === 'Data consumed') {
                    return [
                      `${Number(value).toLocaleString(
                        'en-US',
                        {
                          maximumFractionDigits: 2,
                        },
                      )} GB`,
                      name,
                    ]
                  }

                  return [value, name]
                }}
                contentStyle={{
                  background: '#111827',
                  border: '1px solid rgba(148, 163, 184, 0.18)',
                  borderRadius: '12px',
                  color: '#f8fafc',
                }}
              />

              <Bar
                dataKey="dataGB"
                name="Data consumed"
                fill="#f59e0b"
                radius={[6, 6, 0, 0]}
                maxBarSize={60}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default UsageTimeChart
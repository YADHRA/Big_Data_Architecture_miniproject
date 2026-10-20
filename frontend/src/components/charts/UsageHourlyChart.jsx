import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

function UsageHourlyChart({ data = [] }) {
  const chartData = [...data]
    .sort((a, b) => a.hour_of_day - b.hour_of_day)
    .map((item) => ({
      hour: `${String(item.hour_of_day).padStart(2, '0')}:00`,
      dataGB: Number(
        (item.total_data_mb / 1000).toFixed(2),
      ),
      sessions: Number(item.sessions),
    }))

  return (
    <div className="usage-chart-card usage-hourly-card">
      <div className="usage-chart-header">
        <div>
          <div className="chart-eyebrow">
            HOURLY TRAFFIC PROFILE
          </div>

          <h2 className="chart-title">
            24-Hour Data Consumption
          </h2>

          <p className="chart-description">
            Total data consumption across each hour of the day.
          </p>
        </div>

        <div className="chart-period-badge">
          24 hours
        </div>
      </div>

      <div className="usage-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No hourly usage data available.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={360}>
            <AreaChart
              data={chartData}
              margin={{
                top: 12,
                right: 18,
                left: 8,
                bottom: 8,
              }}
            >
              <defs>
                <linearGradient
                  id="hourlyUsageGradient"
                  x1="0"
                  y1="0"
                  x2="0"
                  y2="1"
                >
                  <stop
                    offset="0%"
                    stopColor="#818cf8"
                    stopOpacity={0.35}
                  />

                  <stop
                    offset="100%"
                    stopColor="#818cf8"
                    stopOpacity={0}
                  />
                </linearGradient>
              </defs>

              <CartesianGrid
                strokeDasharray="3 3"
                stroke="rgba(148, 163, 184, 0.08)"
                vertical={false}
              />

              <XAxis
                dataKey="hour"
                tick={{
                  fill: '#64748b',
                  fontSize: 10,
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
                tickFormatter={(value) => `${value} GB`}
              />

              <Tooltip
                cursor={{
                  stroke: 'rgba(129, 140, 248, 0.35)',
                }}
                formatter={(value) => [
                  `${Number(value).toLocaleString(
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

              <Area
                type="monotone"
                dataKey="dataGB"
                stroke="#818cf8"
                strokeWidth={2.5}
                fill="url(#hourlyUsageGradient)"
                dot={false}
                activeDot={{
                  r: 5,
                  strokeWidth: 2,
                  stroke: '#c7d2fe',
                  fill: '#6366f1',
                }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

export default UsageHourlyChart
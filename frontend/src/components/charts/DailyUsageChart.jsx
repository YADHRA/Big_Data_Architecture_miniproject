import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

function DailyUsageChart({ data = [] }) {
  const chartData = data.map((item) => ({
    date: item.usage_date,
    dataGB: Number((item.total_data_mb / 1000).toFixed(2)),
  }))

  return (
    <div className="chart-card">
      <div className="chart-card-header">
        <div>
          <div className="chart-eyebrow">
            TRAFFIC TREND
          </div>

          <h3 className="chart-title">
            Daily Data Consumption
          </h3>

          <p className="chart-description">
            Total internet data consumed across all usage
            sessions for each day.
          </p>
        </div>

        <div className="chart-period">
          {data.length} days
        </div>
      </div>

      <div className="chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No daily usage data available.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={320}>
            <AreaChart
              data={chartData}
              margin={{
                top: 12,
                right: 12,
                left: 8,
                bottom: 4,
              }}
            >
              <defs>
                <linearGradient
                  id="dailyDataGradient"
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
                stroke="rgba(148, 163, 184, 0.10)"
                vertical={false}
              />

              <XAxis
                dataKey="date"
                tick={{
                  fill: '#64748b',
                  fontSize: 11,
                }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) =>
                  value.slice(5)
                }
              />

              <YAxis
                tick={{
                  fill: '#64748b',
                  fontSize: 11,
                }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) =>
                  `${value} GB`
                }
              />

              <Tooltip
                cursor={{
                  stroke: 'rgba(129, 140, 248, 0.35)',
                  strokeWidth: 1,
                }}
                contentStyle={{
                  background: '#111827',
                  border:
                    '1px solid rgba(148, 163, 184, 0.18)',
                  borderRadius: '12px',
                  color: '#f8fafc',
                  boxShadow:
                    '0 12px 30px rgba(0, 0, 0, 0.30)',
                }}
                labelStyle={{
                  color: '#cbd5e1',
                  marginBottom: '4px',
                }}
                formatter={(value) => [
                  `${Number(value).toLocaleString(
                    'en-US',
                    {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    }
                  )} GB`,
                  'Data consumed',
                ]}
              />

              <Area
                type="monotone"
                dataKey="dataGB"
                stroke="#818cf8"
                strokeWidth={2.5}
                fill="url(#dailyDataGradient)"
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

export default DailyUsageChart
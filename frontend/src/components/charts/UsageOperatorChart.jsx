import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

function UsageOperatorChart({ data = [] }) {
  const chartData = [...data]
    .sort((a, b) => b.total_data_mb - a.total_data_mb)
    .map((item) => ({
      name: item.operator,
      dataGB: Number(
        (item.total_data_mb / 1000).toFixed(2),
      ),
    }))

  return (
    <div className="usage-chart-card">
      <div className="usage-chart-header">
        <div>
          <div className="chart-eyebrow">
            OPERATOR COMPARISON
          </div>

          <h2 className="chart-title">
            Data Consumption by Operator
          </h2>

          <p className="chart-description">
            Total internet data associated with each operator.
          </p>
        </div>
      </div>

      <div className="usage-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No operator usage data available.
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
                formatter={(value) => [
                  `${Number(value).toLocaleString('en-US', {
                    maximumFractionDigits: 2,
                  })} GB`,
                  'Data consumed',
                ]}
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
                fill="#22c55e"
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

export default UsageOperatorChart
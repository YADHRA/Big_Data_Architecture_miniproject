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
  0: 'Moderate-Data Multi-App Customers',
  1: 'Low-Data Single-App Customers',
  2: 'High-Data Customers',
}

function CustomerClusterChart({ data = [] }) {
  const chartData = data.map((item) => ({
    name: `C${item.cluster}`,
    cluster: item.cluster_name || clusterNames[item.cluster] || `Cluster ${item.cluster}`,
    customers: Number(item.customer_count),
  }))

  return (
    <div className="segment-chart-card">
      <div className="segment-chart-header">
        <div>
          <div className="chart-eyebrow">CUSTOMER SEGMENTATION</div>
          <h2 className="chart-title">Customer Cluster Distribution</h2>
          <p className="chart-description">
            Repeat customers grouped by their usage characteristics.
          </p>
        </div>
      </div>

      <div className="segment-chart-container">
        {chartData.length === 0 ? (
          <div className="chart-empty">
            No customer cluster data available.
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
                  'Customers',
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
                dataKey="customers"
                name="Customers"
                fill="#22c55e"
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

export default CustomerClusterChart
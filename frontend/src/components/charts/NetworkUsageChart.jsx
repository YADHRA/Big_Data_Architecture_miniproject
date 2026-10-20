import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
} from 'recharts'

const COLORS = [
  '#7c83fd',
  '#22c55e',
  '#f59e0b',
  '#ef4444',
]

function NetworkUsageChart({ data = [] }) {
  const chartData = data.map((item) => ({
    name: item.network_type,
    value: Number(item.total_data_mb),
  }))

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <div className="chart-eyebrow">
            NETWORK DISTRIBUTION
          </div>

          <h2 className="chart-title">
            Data Consumption by Network
          </h2>

          <p className="chart-description">
            Share of total data consumption across
            network types.
          </p>
        </div>
      </div>

      <div className="chart-container network-chart">
        <ResponsiveContainer
          width="100%"
          height={320}
        >
          <PieChart>
            <Pie
              data={chartData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="45%"
              innerRadius={75}
              outerRadius={110}
              paddingAngle={3}
              stroke="none"
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${entry.name}`}
                  fill={
                    COLORS[index % COLORS.length]
                  }
                />
              ))}
            </Pie>

            <Tooltip
              formatter={(value) => [
                `${Number(
                  value / 1000
                ).toLocaleString(
                  'en-US',
                  {
                    maximumFractionDigits: 2,
                  }
                )} GB`,
                'Data',
              ]}
              contentStyle={{
                background:
                  'rgba(15, 23, 42, 0.96)',
                border:
                  '1px solid rgba(148, 163, 184, 0.18)',
                borderRadius: '10px',
                color: '#fff',
              }}
            />

            <Legend
              verticalAlign="bottom"
              height={36}
              iconType="circle"
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

export default NetworkUsageChart

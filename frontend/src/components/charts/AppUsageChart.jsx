import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

function AppUsageChart({ data = [] }) {
  const chartData = [...data]
    .sort((a, b) => b.total_data_mb - a.total_data_mb)
    .map((item) => ({
      app_name: item.app_name,
      total_data_gb: Number(
        (item.total_data_mb / 1000).toFixed(2)
      ),
    }))

  return (
    <div className="chart-card">
      <div className="chart-header">
        <div>
          <div className="chart-eyebrow">
            APPLICATION USAGE
          </div>

          <h2 className="chart-title">
            Data Consumption by Application
          </h2>

          <p className="chart-description">
            Total data consumed across applications.
          </p>
        </div>
      </div>

      <div className="chart-container">
        <ResponsiveContainer
          width="100%"
          height={320}
        >
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
              opacity={0.08}
            />

            <XAxis
              dataKey="app_name"
              tick={{
                fontSize: 11,
              }}
              tickLine={false}
              axisLine={false}
            />

            <YAxis
              tick={{
                fontSize: 11,
              }}
              tickLine={false}
              axisLine={false}
              tickFormatter={(value) =>
                `${value} GB`
              }
            />

            <Tooltip
              formatter={(value) => [
                `${value} GB`,
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

            <Bar
              dataKey="total_data_gb"
              name="Total Data"
              fill="#7c83fd"
              radius={[6, 6, 0, 0]}
              maxBarSize={55}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

export default AppUsageChart

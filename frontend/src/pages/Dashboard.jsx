import { useEffect, useState } from 'react'
import {
  Activity,
  AlertTriangle,
  Database,
  Users,
} from 'lucide-react'

import {
  getKPIs,
  getUsageDaily,
  getUsageByApps,
  getUsageByNetworks,
} from '../services/api'

import KPICard from '../components/cards/KPICard'
import InsightCard from '../components/cards/InsightCard'
import PageHeader from '../components/common/PageHeader'
import LoadingState from '../components/common/LoadingState'
import ErrorState from '../components/common/ErrorState'

import DailyUsageChart from '../components/charts/DailyUsageChart'
import AppUsageChart from '../components/charts/AppUsageChart'
import NetworkUsageChart from '../components/charts/NetworkUsageChart'

function Dashboard() {
  const [kpis, setKpis] = useState(null)
  const [dailyUsage, setDailyUsage] = useState([])
  const [apps, setApps] = useState([])
  const [networks, setNetworks] = useState([])

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const loadDashboard = async () => {
    try {
      setLoading(true)
      setError('')

      const [
        kpiData,
        dailyData,
        appData,
        networkData,
      ] = await Promise.all([
        getKPIs(),
        getUsageDaily(),
        getUsageByApps(),
        getUsageByNetworks(),
      ])

      setKpis(kpiData)
      setDailyUsage(dailyData)
      setApps(appData)
      setNetworks(networkData)
    } catch (err) {
      console.error('Dashboard loading error:', err)

      setError(
        'The dashboard could not retrieve analytics from the FastAPI backend.',
      )
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadDashboard()
  }, [])

  // ------------------------------------------------------------
  // Loading state
  // ------------------------------------------------------------

  if (loading) {
    return (
      <div className="page-content">
        <PageHeader
          eyebrow="Analytics overview"
          title="Internet Usage Intelligence"
          description="A consolidated view of application usage, consumption patterns and detected usage anomalies."
        />

        <LoadingState message="Loading genuine Spark-derived analytics..." />
      </div>
    )
  }

  // ------------------------------------------------------------
  // Error state
  // ------------------------------------------------------------

  if (error) {
    return (
      <div className="page-content">
        <PageHeader
          eyebrow="Analytics overview"
          title="Internet Usage Intelligence"
          description="A consolidated view of application usage, consumption patterns and detected usage anomalies."
        />

        <ErrorState
          message={error}
          onRetry={loadDashboard}
        />
      </div>
    )
  }

  // ------------------------------------------------------------
  // Data formatting
  // ------------------------------------------------------------

  /*
   * Backend stores total_data_mb in MB.
   *
   * 1 GB = 1024 MB
   * 1 TB = 1024 GB
   *
   * Current project value:
   * 101,000,617.12 MB
   * ≈ 96.3 TB
   */
  const totalDataTB =
    kpis.total_data_mb / (1024 * 1024)

  const formattedTotalDataTB =
    totalDataTB.toLocaleString(undefined, {
      maximumFractionDigits: 1,
    })

  const anomalyRate =
    Number(kpis.anomaly_pct || 0).toFixed(2)

  return (
    <div className="page-content dashboard-page">

      {/* ======================================================
          PAGE HEADER
          ====================================================== */}

      <PageHeader
        eyebrow="Analytics overview"
        title="Internet Usage Intelligence"
        description="Monitor consumption patterns, application behavior and usage anomalies across the analyzed telecom dataset."
      />

      {/* ======================================================
          KPI SECTION
          ====================================================== */}

      <section className="dashboard-kpi-grid">

        <KPICard
          label="Total sessions"
          value={kpis.total_sessions.toLocaleString()}
          helper="Analyzed usage sessions"
          icon={<Activity size={18} />}
        />

        <KPICard
          label="Unique customers"
          value={kpis.unique_customers.toLocaleString()}
          helper="Distinct customer IDs"
          icon={<Users size={18} />}
        />

        <KPICard
          label="Total data"
          value={`${formattedTotalDataTB} TB`}
          helper="Across all analyzed sessions"
          icon={<Database size={18} />}
        />

        <KPICard
          label="Usage anomalies"
          value={kpis.total_anomalies.toLocaleString()}
          helper={`${anomalyRate}% of sessions`}
          icon={<AlertTriangle size={18} />}
          tone="warning"
        />

      </section>

      {/* ======================================================
          PRIMARY CHART
          ====================================================== */}

      <section className="dashboard-primary-section">

        <div className="section-heading">

          <div>

            <span className="section-eyebrow">
              Consumption trend
            </span>

            <h2>
              Daily data consumption
            </h2>

            <p>
              Total data consumed across all application
              sessions by date.
            </p>

          </div>

        </div>

        <div className="chart-card chart-card-large">

          <DailyUsageChart
            data={dailyUsage}
          />

        </div>

      </section>

      {/* ======================================================
          SECONDARY CHARTS
          ====================================================== */}

      <section className="dashboard-secondary-grid">

        {/* Application usage */}

        <div className="chart-card">

          <div className="chart-card-header">

            <div>

              <span className="section-eyebrow">
                Applications
              </span>

              <h2>
                Consumption by application
              </h2>

            </div>

          </div>

          <AppUsageChart
            data={apps}
          />

        </div>

        {/* Network usage */}

        <div className="chart-card">

          <div className="chart-card-header">

            <div>

              <span className="section-eyebrow">
                Network type
              </span>

              <h2>
                Data usage by network type
              </h2>

            </div>

          </div>

          <NetworkUsageChart
            data={networks}
          />

        </div>

      </section>

      {/* ======================================================
          PLATFORM INSIGHTS
          ====================================================== */}

      <section className="dashboard-insights">

        <div className="section-heading">

          <div>

            <span className="section-eyebrow">
              Platform notes
            </span>

            <h2>
              What this dashboard represents
            </h2>

            <p>
              The dashboard presents measured outputs from
              the Spark, Hive and machine-learning pipeline.
            </p>

          </div>

        </div>

        <div className="insight-grid">

          {/* Data source */}

          <InsightCard
            title="Data source"
            value="Synthetic telecom dataset"
            description="The underlying dataset is synthetic and is used to simulate application-level internet consumption patterns."
          />

          {/* Anomaly methodology */}

          <InsightCard
            title="Anomaly methodology"
            value="Category-specific IQR"
            description="Usage anomalies are identified using category-specific upper bounds. They represent unusual consumption patterns and should not be interpreted as confirmed fraud."
            tone="warning"
          />

          {/* Architecture */}

          <InsightCard
            title="Analytics architecture"
            value="Spark → Hive → FastAPI"
            description="React consumes prepared analytics generated by the data engineering and machine-learning pipeline rather than recalculating the Spark workflow for every request."
            tone="success"
          />

        </div>

      </section>

    </div>
  )
}

export default Dashboard
import { useEffect, useState } from 'react'
import {
  Activity,
  BarChart3,
  Clock3,
  Database,
  AlertTriangle,
} from 'lucide-react'

import {
  getUsageByApps,
  getUsageByCategories,
  getUsageByOperators,
  getUsageByNetworks,
  getUsageByTimeOfDay,
  getUsageHourly,
} from '../services/api'

import UsageAppChart from '../components/charts/UsageAppChart'
import UsageCategoryChart from '../components/charts/UsageCategoryChart'
import UsageOperatorChart from '../components/charts/UsageOperatorChart'
import UsageNetworkChart from '../components/charts/UsageNetworkChart'
import UsageTimeChart from '../components/charts/UsageTimeChart'
import UsageHourlyChart from '../components/charts/UsageHourlyChart'

function formatNumber(value) {
  return new Intl.NumberFormat('en-US').format(value)
}

function Usage() {
  const [apps, setApps] = useState([])
  const [categories, setCategories] = useState([])
  const [operators, setOperators] = useState([])
  const [networks, setNetworks] = useState([])
  const [timeOfDay, setTimeOfDay] = useState([])
  const [hourly, setHourly] = useState([])

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let mounted = true

    async function loadUsageAnalytics() {
      try {
        setLoading(true)
        setError(null)

        const [
          appData,
          categoryData,
          operatorData,
          networkData,
          timeData,
          hourlyData,
        ] = await Promise.all([
          getUsageByApps(),
          getUsageByCategories(),
          getUsageByOperators(),
          getUsageByNetworks(),
          getUsageByTimeOfDay(),
          getUsageHourly(),
        ])

        if (!mounted) {
          return
        }

        setApps(Array.isArray(appData) ? appData : [])
        setCategories(
          Array.isArray(categoryData) ? categoryData : [],
        )
        setOperators(
          Array.isArray(operatorData) ? operatorData : [],
        )
        setNetworks(
          Array.isArray(networkData) ? networkData : [],
        )
        setTimeOfDay(
          Array.isArray(timeData) ? timeData : [],
        )
        setHourly(
          Array.isArray(hourlyData) ? hourlyData : [],
        )
      } catch (err) {
        if (mounted) {
          setError(
            err?.message ||
              'Unable to load usage analytics.',
          )
        }
      } finally {
        if (mounted) {
          setLoading(false)
        }
      }
    }

    loadUsageAnalytics()

    return () => {
      mounted = false
    }
  }, [])

  /*
   * LOADING STATE
   */
  if (loading) {
    return (
      <div className="usage-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              ANALYTICS
            </div>

            <h1 className="page-title">
              Usage Analytics
            </h1>

            <p className="page-description">
              Loading detailed usage analytics...
            </p>
          </div>

          <div className="analytics-status">
            <span className="status-dot" />
            Connecting to API
          </div>
        </div>

        <div className="loading-state">
          <div className="loading-spinner" />

          <span>
            Loading usage data from the analytics API...
          </span>
        </div>
      </div>
    )
  }

  /*
   * ERROR STATE
   */
  if (error) {
    return (
      <div className="usage-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              ANALYTICS
            </div>

            <h1 className="page-title">
              Usage Analytics
            </h1>

            <p className="page-description">
              Detailed analysis of internet usage
              patterns.
            </p>
          </div>

          <div className="analytics-status analytics-status-error">
            <span className="status-dot" />
            API Error
          </div>
        </div>

        <div className="error-state">
          <AlertTriangle size={20} />

          <div>
            <strong>
              Analytics unavailable
            </strong>

            <p>
              {error}
            </p>

            <small>
              Make sure the FastAPI backend is running on
              port 8000.
            </small>
          </div>
        </div>
      </div>
    )
  }

  /*
   * MAIN PAGE
   */
  return (
    <div className="usage-page">

      {/* =====================================================
          PAGE HEADER
          ===================================================== */}

      <div className="page-heading">

        <div>
          <div className="page-eyebrow">
            ANALYTICS
          </div>

          <h1 className="page-title">
            Usage Analytics
          </h1>

          <p className="page-description">
            Explore application, category, operator,
            network and time-based internet usage patterns.
          </p>
        </div>

        <div className="analytics-status">
          <span className="status-dot" />
          Live API Data
        </div>

      </div>


      {/* =====================================================
          SUMMARY CARDS
          ===================================================== */}

      <section className="usage-summary-grid">

        <div className="usage-summary-card">

          <div className="usage-summary-icon">
            <BarChart3 size={18} />
          </div>

          <div>
            <span>
              Applications
            </span>

            <strong>
              {apps.length}
            </strong>
          </div>

        </div>


        <div className="usage-summary-card">

          <div className="usage-summary-icon">
            <Database size={18} />
          </div>

          <div>
            <span>
              Categories
            </span>

            <strong>
              {categories.length}
            </strong>
          </div>

        </div>


        <div className="usage-summary-card">

          <div className="usage-summary-icon">
            <Activity size={18} />
          </div>

          <div>
            <span>
              Operators
            </span>

            <strong>
              {operators.length}
            </strong>
          </div>

        </div>


        <div className="usage-summary-card">

          <div className="usage-summary-icon">
            <Database size={18} />
          </div>

          <div>
            <span>
              Network Types
            </span>

            <strong>
              {networks.length}
            </strong>
          </div>

        </div>


        <div className="usage-summary-card">

          <div className="usage-summary-icon">
            <Clock3 size={18} />
          </div>

          <div>
            <span>
              Time-of-day Buckets
            </span>

            <strong>
              {timeOfDay.length}
            </strong>
          </div>

        </div>

      </section>


      {/* =====================================================
          DATA COVERAGE
          ===================================================== */}

      <section className="analytics-overview-card">

        <div>

          <div className="chart-eyebrow">
            DATA COVERAGE
          </div>

          <h2>
            Usage dataset successfully connected
          </h2>

          <p>
            Analytics are served from the validated
            Spark processing pipeline through the
            FastAPI backend.
          </p>

        </div>


        <div className="analytics-overview-stats">

          <div>
            <span>
              Applications
            </span>

            <strong>
              {formatNumber(apps.length)}
            </strong>
          </div>


          <div>
            <span>
              Hourly buckets
            </span>

            <strong>
              {formatNumber(hourly.length)}
            </strong>
          </div>


          <div>
            <span>
              Network types
            </span>

            <strong>
              {formatNumber(networks.length)}
            </strong>
          </div>

        </div>

      </section>


      {/* =====================================================
          USAGE ANALYTICS
          ===================================================== */}

      <section className="usage-chart-section">

        {/* Application usage */}
        <UsageAppChart
          data={apps}
        />


        {/* Category usage */}
        <UsageCategoryChart
          data={categories}
        />


        {/* Operator usage */}
        <UsageOperatorChart
          data={operators}
        />


        {/* Network usage */}
        <UsageNetworkChart
          data={networks}
        />


        {/* Time-of-day usage */}
        <UsageTimeChart
          data={timeOfDay}
        />


        {/* Hourly usage */}
        <UsageHourlyChart
          data={hourly}
        />

      </section>

    </div>
  )
}

export default Usage
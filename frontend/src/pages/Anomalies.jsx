import { useEffect, useMemo, useState } from 'react'
import {
  AlertTriangle,
  Search,
  ShieldAlert,
} from 'lucide-react'

import {
  getKPIs,
  getAnomalies,
  getAnomalyThresholds,
} from '../services/api'

import AnomalyCategoryChart from '../components/charts/AnomalyCategoryChart'
import AnomalyThresholdChart from '../components/charts/AnomalyThresholdChart'

function formatNumber(value) {
  return new Intl.NumberFormat('en-US').format(value)
}

function formatDecimal(value, digits = 2) {
  return Number(value).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function Anomalies() {
  const [kpis, setKpis] = useState(null)
  const [anomalies, setAnomalies] = useState([])
  const [thresholds, setThresholds] = useState([])

  const [search, setSearch] = useState('')

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let mounted = true

    const loadAnomalyData = async () => {
      try {
        setLoading(true)
        setError(null)

        const [
          kpiData,
          anomalyData,
          thresholdData,
        ] = await Promise.all([
          getKPIs(),
          getAnomalies(),
          getAnomalyThresholds(),
        ])

        if (!mounted) {
          return
        }

        setKpis(kpiData)
        setAnomalies(anomalyData)
        setThresholds(thresholdData)
      } catch (err) {
        if (mounted) {
          setError(
            err?.message ||
              'Unable to load anomaly analytics.',
          )
        }
      } finally {
        if (mounted) {
          setLoading(false)
        }
      }
    }

    loadAnomalyData()

    return () => {
      mounted = false
    }
  }, [])

  const filteredAnomalies = useMemo(() => {
    const query = search.trim().toLowerCase()

    if (!query) {
      return anomalies.slice(0, 100)
    }

    return anomalies
      .filter((item) => {
        return [
          item.session_id,
          item.customer_id,
          item.operator,
          item.app_name,
          item.app_category,
          item.network_type,
        ]
          .join(' ')
          .toLowerCase()
          .includes(query)
      })
      .slice(0, 100)
  }, [anomalies, search])

  if (loading) {
    return (
      <div className="anomalies-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              MONITORING
            </div>

            <h1 className="page-title">
              Usage Anomalies
            </h1>

            <p className="page-description">
              Loading category-specific usage anomaly analysis...
            </p>
          </div>
        </div>

        <div className="loading-state">
          <div className="loading-spinner" />
          <span>
            Loading anomaly data...
          </span>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="anomalies-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              MONITORING
            </div>

            <h1 className="page-title">
              Usage Anomalies
            </h1>

            <p className="page-description">
              Detailed analysis of unusual usage patterns.
            </p>
          </div>
        </div>

        <div className="error-state">
          <AlertTriangle size={20} />

          <div>
            <strong>
              Anomaly analytics unavailable
            </strong>

            <p>{error}</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="anomalies-page">

      {/* PAGE HEADER */}

      <div className="page-heading">
        <div>
          <div className="page-eyebrow">
            MONITORING
          </div>

          <h1 className="page-title">
            Usage Anomalies
          </h1>

          <p className="page-description">
            Identify unusually high data-consumption
            sessions using category-specific statistical
            thresholds.
          </p>
        </div>

        <div className="anomaly-status">
          <span className="anomaly-status-dot" />
          IQR Detection Active
        </div>
      </div>

      {/* KPI CARDS */}

      <section className="anomaly-summary-grid">

        <div className="anomaly-summary-card">
          <span className="anomaly-summary-label">
            Total Sessions
          </span>

          <strong className="anomaly-summary-value">
            {formatNumber(kpis?.total_sessions || 0)}
          </strong>

          <span className="anomaly-summary-subtitle">
            Analyzed usage sessions
          </span>
        </div>

        <div className="anomaly-summary-card">
          <span className="anomaly-summary-label">
            Detected Anomalies
          </span>

          <strong className="anomaly-summary-value">
            {formatNumber(anomalies.length)}
          </strong>

          <span className="anomaly-summary-subtitle">
            Sessions above category bounds
          </span>
        </div>

        <div className="anomaly-summary-card">
          <span className="anomaly-summary-label">
            Anomaly Rate
          </span>

          <strong className="anomaly-summary-value">
            {formatDecimal(
              kpis?.anomaly_pct || 0,
              4,
            )}
            %
          </strong>

          <span className="anomaly-summary-subtitle">
            Share of all analyzed sessions
          </span>
        </div>

        <div className="anomaly-summary-card">
          <span className="anomaly-summary-label">
            Categories Monitored
          </span>

          <strong className="anomaly-summary-value">
            {thresholds.length}
          </strong>

          <span className="anomaly-summary-subtitle">
            Independent IQR thresholds
          </span>
        </div>

      </section>

      {/* METHOD EXPLANATION */}

      <section className="anomaly-method-card">

        <div className="anomaly-method-icon">
          <ShieldAlert size={20} />
        </div>

        <div>
          <h2>
            How anomaly detection works
          </h2>

          <p>
            For each application category, the system
            calculates the third quartile (Q3) and
            interquartile range (IQR). A session is flagged
            when its data consumption exceeds Q3 + 1.5 × IQR.
            Anomalous sessions are retained for investigation
            rather than removed from the dataset. Because this
            dataset does not contain ground-truth fraud labels,
            these results represent unusual usage patterns,
            not confirmed fraud.
          </p>
        </div>

      </section>

      {/* CHARTS */}

      <section className="anomaly-chart-grid">

        <AnomalyCategoryChart
          data={anomalies}
        />

        <AnomalyThresholdChart
          data={thresholds}
        />

      </section>

      {/* ANOMALY TABLE */}

      <section className="anomaly-table-card">

        <div className="anomaly-table-header">

          <div>
            <h2 className="anomaly-table-title">
              Detected Usage Anomalies
            </h2>

            <p className="anomaly-table-subtitle">
              Showing up to 100 matching sessions from{' '}
              {formatNumber(anomalies.length)} detected
              anomalies.
            </p>
          </div>

          <div style={{ position: 'relative' }}>

            <Search
              size={15}
              style={{
                position: 'absolute',
                left: '11px',
                top: '50%',
                transform: 'translateY(-50%)',
                color: '#64748b',
              }}
            />

            <input
              className="anomaly-search"
              style={{ paddingLeft: '34px' }}
              type="text"
              placeholder="Search session, customer, app..."
              value={search}
              onChange={(event) =>
                setSearch(event.target.value)
              }
            />

          </div>

        </div>

        <div className="anomaly-table-wrapper">

          <table className="anomaly-table">

            <thead>
              <tr>
                <th>Session</th>
                <th>Timestamp</th>
                <th>Application</th>
                <th>Category</th>
                <th>Data</th>
                <th>Threshold</th>
                <th>Ratio</th>
                <th>Network</th>
              </tr>
            </thead>

            <tbody>

              {filteredAnomalies.map((item) => (
                <tr key={item.session_id}>

                  <td className="anomaly-session-id">
                    {item.session_id}
                  </td>

                  <td>
                    {item.timestamp}
                  </td>

                  <td>
                    {item.app_name}
                  </td>

                  <td>
                    {item.app_category}
                  </td>

                  <td className="anomaly-value">
                    {formatDecimal(
                      item.data_consumed_mb,
                      2,
                    )}{' '}
                    MB
                  </td>

                  <td>
                    {formatDecimal(
                      item.anomaly_threshold_mb,
                      2,
                    )}{' '}
                    MB
                  </td>

                  <td>
                    <span className="anomaly-ratio">
                      {formatDecimal(
                        item.ratio_to_threshold,
                        2,
                      )}
                      ×
                    </span>
                  </td>

                  <td>
                    {item.network_type}
                  </td>

                </tr>
              ))}

            </tbody>

          </table>

        </div>

        <div className="anomaly-table-footer">
          Showing {filteredAnomalies.length} rows.
          Anomalous sessions are preserved and flagged
          rather than removed.
        </div>

      </section>

    </div>
  )
}

export default Anomalies
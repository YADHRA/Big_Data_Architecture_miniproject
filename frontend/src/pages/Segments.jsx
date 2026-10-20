import { useEffect, useMemo, useState } from 'react'
import {
  Users,
  Activity,
  Layers3,
  AlertTriangle,
} from 'lucide-react'

import {
  getSessionClusters,
  getSessionClustersByApp,
  getCustomerClusters,
} from '../services/api'

import SessionClusterChart from '../components/charts/SessionClusterChart'
import CustomerClusterChart from '../components/charts/CustomerClusterChart'

const sessionClusterNames = {
  0: 'Long Social Sessions',
  1: 'High-Data Streaming Sessions',
  2: 'Short Social Sessions',
}

const customerClusterNames = {
  0: 'Moderate-Data Multi-App Customers',
  1: 'Low-Data Single-App Customers',
  2: 'High-Data Customers',
}

function formatNumber(value) {
  return new Intl.NumberFormat('en-US').format(value)
}

function formatDecimal(value, digits = 2) {
  return Number(value).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function Segments() {
  const [sessionClusters, setSessionClusters] = useState([])
  const [sessionClustersByApp, setSessionClustersByApp] = useState([])
  const [customerClusters, setCustomerClusters] = useState([])

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let mounted = true

    const loadSegmentData = async () => {
      try {
        setLoading(true)
        setError(null)

        const [
          sessionData,
          sessionAppData,
          customerData,
        ] = await Promise.all([
          getSessionClusters(),
          getSessionClustersByApp(),
          getCustomerClusters(),
        ])

        if (!mounted) {
          return
        }

        setSessionClusters(sessionData)
        setSessionClustersByApp(sessionAppData)
        setCustomerClusters(customerData)
      } catch (err) {
        if (mounted) {
          setError(
            err?.message ||
              'Unable to load segmentation analytics.',
          )
        }
      } finally {
        if (mounted) {
          setLoading(false)
        }
      }
    }

    loadSegmentData()

    return () => {
      mounted = false
    }
  }, [])

  const totalSessionClusters = useMemo(
    () =>
      sessionClusters.reduce(
        (sum, item) => sum + Number(item.sessions || 0),
        0,
      ),
    [sessionClusters],
  )

  const totalCustomerClusters = useMemo(
    () =>
      customerClusters.reduce(
        (sum, item) => sum + Number(item.customer_count || 0),
        0,
      ),
    [customerClusters],
  )

  const dominantSessionCluster = useMemo(() => {
    if (!sessionClusters.length) {
      return null
    }

    return [...sessionClusters].sort(
      (a, b) => Number(b.sessions) - Number(a.sessions),
    )[0]
  }, [sessionClusters])

  const dominantCustomerCluster = useMemo(() => {
    if (!customerClusters.length) {
      return null
    }

    return [...customerClusters].sort(
      (a, b) =>
        Number(b.customer_count) -
        Number(a.customer_count),
    )[0]
  }, [customerClusters])

  if (loading) {
    return (
      <div className="segments-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              SEGMENTATION
            </div>

            <h1 className="page-title">
              Customer & Session Segments
            </h1>

            <p className="page-description">
              Loading K-Means segmentation results...
            </p>
          </div>
        </div>

        <div className="loading-state">
          <div className="loading-spinner" />
          <span>
            Loading segmentation data...
          </span>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="segments-page">
        <div className="page-heading">
          <div>
            <div className="page-eyebrow">
              SEGMENTATION
            </div>

            <h1 className="page-title">
              Customer & Session Segments
            </h1>

            <p className="page-description">
              K-Means segmentation insights from the analytics pipeline.
            </p>
          </div>
        </div>

        <div className="error-state">
          <AlertTriangle size={20} />

          <div>
            <strong>
              Segmentation analytics unavailable
            </strong>

            <p>{error}</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="segments-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">
            SEGMENTATION
          </div>

          <h1 className="page-title">
            Customer & Session Segments
          </h1>

          <p className="page-description">
            Explore usage segments generated from the project's K-Means clustering pipeline.
          </p>
        </div>

        <div className="segment-status">
          <span className="segment-status-dot" />
          K-Means Segmentation
        </div>
      </div>

      {/* SUMMARY */}
      <section className="segment-summary-grid">
        <div className="segment-summary-card">
          <div className="segment-summary-icon">
            <Activity size={18} />
          </div>

          <span className="segment-summary-label">
            Session Clusters
          </span>

          <strong className="segment-summary-value">
            {sessionClusters.length}
          </strong>

          <span className="segment-summary-subtitle">
            {formatNumber(totalSessionClusters)} analyzed sessions
          </span>
        </div>

        <div className="segment-summary-card">
          <div className="segment-summary-icon">
            <Users size={18} />
          </div>

          <span className="segment-summary-label">
            Customer Clusters
          </span>

          <strong className="segment-summary-value">
            {customerClusters.length}
          </strong>

          <span className="segment-summary-subtitle">
            {formatNumber(totalCustomerClusters)} repeat customers
          </span>
        </div>

        <div className="segment-summary-card">
          <div className="segment-summary-icon">
            <Layers3 size={18} />
          </div>

          <span className="segment-summary-label">
            Session Features
          </span>

          <strong className="segment-summary-value">
            10
          </strong>

          <span className="segment-summary-subtitle">
            Engineered ML dimensions
          </span>
        </div>

        <div className="segment-summary-card">
          <div className="segment-summary-icon">
            <Users size={18} />
          </div>

          <span className="segment-summary-label">
            Customer Features
          </span>

          <strong className="segment-summary-value">
            4
          </strong>

          <span className="segment-summary-subtitle">
            Selected behavioral features
          </span>
        </div>
      </section>

      {/* SESSION SEGMENTATION */}
      <section className="segment-section">
        <div className="segment-section-heading">
          <div>
            <div className="chart-eyebrow">
              SESSION LEVEL
            </div>

            <h2 className="segment-section-title">
              Session Segmentation
            </h2>

            <p className="segment-section-description">
              Sessions were grouped using the selected standardized session-level features.
            </p>
          </div>
        </div>

        <div className="segment-chart-grid">
          <SessionClusterChart data={sessionClusters} />

          <div className="segment-profile-card">
            <div className="chart-eyebrow">
              CLUSTER PROFILES
            </div>

            <h2 className="chart-title">
              Session Cluster Profiles
            </h2>

            <p className="chart-description">
              Key characteristics of each session segment.
            </p>

            <div className="cluster-profile-list">
              {sessionClusters.map((cluster) => (
                <div
                  className="cluster-profile-item"
                  key={cluster.session_cluster}
                >
                  <div className="cluster-profile-top">
                    <div>
                      <span className="cluster-code">
                        C{cluster.session_cluster}
                      </span>

                      <strong>
                        {sessionClusterNames[
                          cluster.session_cluster
                        ] ||
                          `Cluster ${cluster.session_cluster}`}
                      </strong>
                    </div>

                    <span className="cluster-share">
                      {formatDecimal(
                        cluster.session_share_pct,
                        2,
                      )}
                      %
                    </span>
                  </div>

                  <div className="cluster-metrics">
                    <div>
                      <span>Sessions</span>
                      <strong>
                        {formatNumber(cluster.sessions)}
                      </strong>
                    </div>

                    <div>
                      <span>Avg data</span>
                      <strong>
                        {formatDecimal(
                          cluster.avg_data_mb,
                        )}{' '}
                        MB
                      </strong>
                    </div>

                    <div>
                      <span>Avg duration</span>
                      <strong>
                        {formatDecimal(
                          cluster.avg_duration_min,
                        )}{' '}
                        min
                      </strong>
                    </div>

                    <div>
                      <span>Anomalies</span>
                      <strong>
                        {formatDecimal(
                          cluster.anomaly_pct,
                          2,
                        )}
                        %
                      </strong>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* SESSION APP MIX */}
      <section className="segment-table-card">
        <div className="segment-table-header">
          <div>
            <div className="chart-eyebrow">
              SESSION × APPLICATION
            </div>

            <h2 className="segment-section-title">
              Application Mix by Session Cluster
            </h2>

            <p className="segment-section-description">
              Shows how applications contribute to each session cluster.
            </p>
          </div>
        </div>

        <div className="segment-table-wrapper">
          <table className="segment-table">
            <thead>
              <tr>
                <th>Cluster</th>
                <th>Application</th>
                <th>Sessions</th>
                <th>Total Data</th>
                <th>Average Data</th>
              </tr>
            </thead>

            <tbody>
              {sessionClustersByApp
                .slice()
                .sort(
                  (a, b) =>
                    Number(b.sessions) -
                    Number(a.sessions),
                )
                .slice(0, 20)
                .map((item) => (
                  <tr
                    key={`${item.session_cluster}-${item.app_name}`}
                  >
                    <td>
                      <span className="cluster-code">
                        C{item.session_cluster}
                      </span>
                    </td>

                    <td>{item.app_name}</td>

                    <td>
                      {formatNumber(item.sessions)}
                    </td>

                    <td>
                      {formatDecimal(
                        item.total_data_mb,
                      )}{' '}
                      MB
                    </td>

                    <td>
                      {formatDecimal(
                        item.avg_data_mb,
                      )}{' '}
                      MB
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* CUSTOMER SEGMENTATION */}
      <section className="segment-section">
        <div className="segment-section-heading">
          <div>
            <div className="chart-eyebrow">
              CUSTOMER LEVEL
            </div>

            <h2 className="segment-section-title">
              Customer Segmentation
            </h2>

            <p className="segment-section-description">
              Repeat customers were grouped using selected customer-level usage features.
            </p>
          </div>
        </div>

        <div className="segment-chart-grid">
          <CustomerClusterChart data={customerClusters} />

          <div className="segment-profile-card">
            <div className="chart-eyebrow">
              CUSTOMER PROFILES
            </div>

            <h2 className="chart-title">
              Customer Cluster Profiles
            </h2>

            <p className="chart-description">
              Usage characteristics of repeat-customer segments.
            </p>

            <div className="cluster-profile-list">
              {customerClusters.map((cluster) => (
                <div
                  className="cluster-profile-item"
                  key={cluster.cluster}
                >
                  <div className="cluster-profile-top">
                    <div>
                      <span className="cluster-code">
                        C{cluster.cluster}
                      </span>

                      <strong>
                        {cluster.cluster_name ||
                          customerClusterNames[
                            cluster.cluster
                          ] ||
                          `Cluster ${cluster.cluster}`}
                      </strong>
                    </div>

                    <span className="cluster-share">
                      {formatDecimal(
                        cluster.customer_share_pct,
                        2,
                      )}
                      %
                    </span>
                  </div>

                  <div className="cluster-metrics">
                    <div>
                      <span>Customers</span>
                      <strong>
                        {formatNumber(
                          cluster.customer_count,
                        )}
                      </strong>
                    </div>

                    <div>
                      <span>Avg data</span>
                      <strong>
                        {formatDecimal(
                          cluster.avg_data_mb,
                        )}{' '}
                        MB
                      </strong>
                    </div>

                    <div>
                      <span>Avg duration</span>
                      <strong>
                        {formatDecimal(
                          cluster.avg_session_duration_min,
                        )}{' '}
                        min
                      </strong>
                    </div>

                    <div>
                      <span>Unique apps</span>
                      <strong>
                        {formatDecimal(
                          cluster.avg_unique_app_count,
                        )}
                      </strong>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* INTERPRETATION */}
      <section className="segment-note-card">
        <div className="segment-note-icon">
          <AlertTriangle size={19} />
        </div>

        <div>
          <h2>
            Interpretation notes
          </h2>

          <p>
            Session clusters describe groups of usage sessions rather than permanent user identities. In this dataset, the long- and short-session groups should be interpreted carefully because their separation is driven substantially by session duration. Customer clustering is limited to customers with at least two sessions, so these segments do not represent the complete customer population. The clustering results are descriptive usage segments, not evidence of customer intent, fraud, or causation.
          </p>
        </div>
      </section>

      {dominantSessionCluster && (
        <div className="segment-insight-row">
          <span>
            Largest session segment:
          </span>

          <strong>
            C{dominantSessionCluster.session_cluster}{' '}
            —{' '}
            {sessionClusterNames[
              dominantSessionCluster.session_cluster
            ] ||
              `Cluster ${dominantSessionCluster.session_cluster}`}
          </strong>
        </div>
      )}

      {dominantCustomerCluster && (
        <div className="segment-insight-row">
          <span>
            Largest repeat-customer segment:
          </span>

          <strong>
            C{dominantCustomerCluster.cluster}{' '}
            —{' '}
            {dominantCustomerCluster.cluster_name ||
              customerClusterNames[
                dominantCustomerCluster.cluster
              ] ||
              `Cluster ${dominantCustomerCluster.cluster}`}
          </strong>
        </div>
      )}
    </div>
  )
}

export default Segments
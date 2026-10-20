const apiRequest = async (endpoint) => {
  const response = await fetch(endpoint)

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status}`)
  }

  return response.json()
}

// Health
export const getHealth = () => apiRequest('/api/health')

// KPIs
export const getKPIs = () => apiRequest('/api/kpis')

// Usage analytics
export const getUsageByApps = () => apiRequest('/api/usage/apps')

export const getUsageByCategories = () =>
  apiRequest('/api/usage/categories')

export const getUsageByOperators = () =>
  apiRequest('/api/usage/operators')

export const getUsageByNetworks = () =>
  apiRequest('/api/usage/networks')

export const getUsageByTimeOfDay = () =>
  apiRequest('/api/usage/time-of-day')

export const getUsageDaily = () =>
  apiRequest('/api/usage/daily')

export const getUsageHourly = () =>
  apiRequest('/api/usage/hourly')

// Anomaly analytics
export const getAnomalies = () =>
  apiRequest('/api/anomalies')

export const getAnomalyThresholds = () =>
  apiRequest('/api/anomalies/thresholds')

// Segmentation
export const getSessionClusters = () =>
  apiRequest('/api/clusters/sessions')

export const getSessionClustersByApp = () =>
  apiRequest('/api/clusters/sessions/by-app')

export const getCustomerClusters = () =>
  apiRequest('/api/clusters/customers')

// Power BI
export const getPowerBIConfig = () =>
  apiRequest('/api/powerbi/config')

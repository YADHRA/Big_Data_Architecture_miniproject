import { Navigate, Route, Routes } from 'react-router-dom'

import AppLayout from './components/layout/AppLayout'

import Dashboard from './pages/Dashboard'
import Usage from './pages/Usage'
import Anomalies from './pages/Anomalies'
import Segments from './pages/Segments'
import PowerBI from './pages/PowerBI'

function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        {/* Default route */}
        <Route
          path="/"
          element={<Navigate to="/dashboard" replace />}
        />

        {/* Main application pages */}
        <Route
          path="/dashboard"
          element={<Dashboard />}
        />

        <Route
          path="/usage"
          element={<Usage />}
        />

        <Route
          path="/anomalies"
          element={<Anomalies />}
        />

        <Route
          path="/segments"
          element={<Segments />}
        />

        <Route
          path="/powerbi"
          element={<PowerBI />}
        />
      </Route>
    </Routes>
  )
}

export default App
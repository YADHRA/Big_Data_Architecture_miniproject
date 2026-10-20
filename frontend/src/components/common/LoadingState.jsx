function LoadingState({ message = 'Loading analytics...' }) {
  return (
    <div className="state-panel">
      <div className="state-spinner" />
      <div>
        <h3>Preparing analytics</h3>
        <p>{message}</p>
      </div>
    </div>
  )
}

export default LoadingState

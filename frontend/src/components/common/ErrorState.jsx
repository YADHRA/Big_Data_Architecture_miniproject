function ErrorState({
  title = 'Unable to load analytics',
  message = 'The backend did not return the expected data.',
  onRetry = null,
}) {
  return (
    <div className="state-panel state-error">
      <div className="state-icon">!</div>

      <div>
        <h3>{title}</h3>
        <p>{message}</p>

        {onRetry && (
          <button
            type="button"
            className="state-retry"
            onClick={onRetry}
          >
            Try again
          </button>
        )}
      </div>
    </div>
  )
}

export default ErrorState

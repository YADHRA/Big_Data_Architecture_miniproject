function KPICard({
  label,
  value,
  helper,
  icon = null,
  tone = 'default',
}) {
  return (
    <article className={`kpi-card kpi-${tone}`}>
      <div className="kpi-card-top">
        <span className="kpi-label">
          {label}
        </span>

        {icon && (
          <span className="kpi-icon">
            {icon}
          </span>
        )}
      </div>

      <div className="kpi-value">
        {value}
      </div>

      {helper && (
        <div className="kpi-helper">
          {helper}
        </div>
      )}
    </article>
  )
}

export default KPICard
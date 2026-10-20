function InsightCard({
  title,
  value,
  description,
  tone = 'default',
}) {
  return (
    <article className={`insight-card insight-${tone}`}>
      <div className="insight-card-title">
        {title}
      </div>

      {value && (
        <div className="insight-card-value">
          {value}
        </div>
      )}

      <p className="insight-card-description">
        {description}
      </p>
    </article>
  )
}

export default InsightCard

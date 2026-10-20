function PageHeader({
  eyebrow,
  title,
  description,
  action = null,
}) {
  return (
    <div className="page-header">
      <div className="page-header-copy">
        {eyebrow && <div className="page-eyebrow">{eyebrow}</div>}

        <h1 className="page-title">{title}</h1>

        {description && (
          <p className="page-description">{description}</p>
        )}
      </div>

      {action && (
        <div className="page-header-action">
          {action}
        </div>
      )}
    </div>
  )
}

export default PageHeader

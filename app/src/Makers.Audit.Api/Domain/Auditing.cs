namespace Makers.Audit.Api.Domain;

/// <summary>
/// Marks an entity whose changes must produce an audit event. The domain only
/// declares *what* is auditable; writing the event is infrastructure's job
/// (see <c>AuditOutboxInterceptor</c>), so no use case can forget it.
/// </summary>
public interface IAuditableEntity
{
    Guid Id { get; }

    /// <summary>Monetary amount exposed to analytics (payload v1 contract).</summary>
    decimal? AuditAmount { get; }
}

/// <summary>Excludes a property from the audit payload (PII, secrets, technical columns).</summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class AuditIgnoreAttribute : Attribute;

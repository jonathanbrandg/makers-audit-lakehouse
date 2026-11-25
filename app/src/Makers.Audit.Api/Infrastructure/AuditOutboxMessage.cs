namespace Makers.Audit.Api.Infrastructure;

/// <summary>
/// Row of dbo.AuditOutbox: the only contract between the core and the lakehouse
/// (captured by SQL Server CDC -> Debezium). Columns must stay backwards compatible;
/// payload changes bump <see cref="SchemaVersion"/>.
/// </summary>
public sealed class AuditOutboxMessage
{
    public const int CurrentSchemaVersion = 1;

    public Guid EventId { get; init; }

    public DateTime OccurredAt { get; init; }

    public required string EntityType { get; init; }

    public required string EntityId { get; init; }

    public required string Action { get; init; }

    public required string Actor { get; init; }

    public int SchemaVersion { get; init; } = CurrentSchemaVersion;

    public string? Payload { get; init; }
}

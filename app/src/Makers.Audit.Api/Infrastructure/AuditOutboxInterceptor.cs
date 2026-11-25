using System.Reflection;
using System.Text.Json;
using Makers.Audit.Api.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Makers.Audit.Api.Infrastructure;

/// <summary>
/// Transactional outbox without dual-write: audit rows are added to the SAME
/// SaveChanges as the business change, so both commit or roll back together.
/// The request never talks to Kafka/the lake: if the pipeline is down the core
/// keeps working and CDC replays from the transaction log later.
/// </summary>
public sealed class AuditOutboxInterceptor(IAuditContext auditContext, TimeProvider clock) : SaveChangesInterceptor
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        AppendAuditEvents(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        AppendAuditEvents(eventData.Context);
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    private void AppendAuditEvents(DbContext? context)
    {
        if (context is null)
        {
            return;
        }

        var occurredAt = clock.GetUtcNow().UtcDateTime;
        var actor = auditContext.Actor;
        var channel = auditContext.Channel;

        // Materialize first: adding outbox rows mutates the change tracker.
        var messages = context.ChangeTracker
            .Entries<IAuditableEntity>()
            .Where(e => e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
            .ToList()
            .Select(entry => ToMessage(entry, occurredAt, actor, channel))
            .OfType<AuditOutboxMessage>()
            .ToList();

        if (messages.Count > 0)
        {
            context.Set<AuditOutboxMessage>().AddRange(messages);
        }
    }

    private static AuditOutboxMessage? ToMessage(
        EntityEntry<IAuditableEntity> entry,
        DateTime occurredAt,
        string actor,
        string channel)
    {
        var changes = Changes(entry);
        if (entry.State == EntityState.Modified && changes.Count == 0)
        {
            return null; // only ignored/technical columns changed
        }

        var action = entry.State switch
        {
            EntityState.Added => "CREATE",
            EntityState.Modified => "UPDATE",
            _ => "DELETE",
        };

        // Payload contract v1 (parsed by dbt stg_audit__events): amount, channel, changes.
        var payload = JsonSerializer.Serialize(
            new { amount = entry.Entity.AuditAmount, channel, changes },
            JsonOptions);

        return new AuditOutboxMessage
        {
            OccurredAt = occurredAt,
            EntityType = entry.Metadata.ClrType.Name,
            EntityId = entry.Entity.Id.ToString(),
            Action = action,
            Actor = actor,
            Payload = payload,
        };
    }

    private static Dictionary<string, object?> Changes(EntityEntry entry)
    {
        var changes = new Dictionary<string, object?>();
        foreach (var property in entry.Properties)
        {
            if (property.Metadata.PropertyInfo?.GetCustomAttribute<AuditIgnoreAttribute>() is not null)
            {
                continue;
            }

            var name = JsonNamingPolicy.CamelCase.ConvertName(property.Metadata.Name);
            switch (entry.State)
            {
                case EntityState.Added:
                    changes[name] = new { @new = property.CurrentValue };
                    break;
                case EntityState.Deleted:
                    changes[name] = new { old = property.OriginalValue };
                    break;
                case EntityState.Modified when property.IsModified && !Equals(property.OriginalValue, property.CurrentValue):
                    changes[name] = new { old = property.OriginalValue, @new = property.CurrentValue };
                    break;
            }
        }

        return changes;
    }
}

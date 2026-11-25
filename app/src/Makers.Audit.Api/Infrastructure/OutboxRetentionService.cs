using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Makers.Audit.Api.Infrastructure;

public sealed class OutboxRetentionOptions
{
    public const string Section = "OutboxRetention";

    public bool Enabled { get; set; } = true;

    /// <summary>Hot window kept in SQL Server. History lives in Iceberg.</summary>
    public int RetentionMinutes { get; set; } = 24 * 60;

    /// <summary>Below SQL Server's ~5,000-lock escalation threshold: no table lock.</summary>
    public int BatchSize { get; set; } = 4000;

    public int IntervalSeconds { get; set; } = 60;
}

/// <summary>
/// Keeps dbo.AuditOutbox small: this is what stops audit tables from degrading the OLTP.
/// A row is deleted only when (1) it is older than the hot window AND (2) CDC already
/// captured it (its insert exists in the change table), so a stalled capture job can
/// never turn retention into data loss. Deletes are small batches in autocommit mode.
/// </summary>
public sealed partial class OutboxRetentionService(
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<OutboxRetentionOptions> options,
    TimeProvider clock,
    ILogger<OutboxRetentionService> logger) : BackgroundService
{
    // cdc.<capture_instance>_CT; capture instance defaults to dbo_AuditOutbox. __$operation 2 = insert.
    private const string DeleteBatchSql = """
        DELETE TOP (@p0) o
        FROM dbo.AuditOutbox AS o
        WHERE o.OccurredAt < @p1
          AND EXISTS (
              SELECT 1 FROM cdc.dbo_AuditOutbox_CT AS ct
              WHERE ct.EventId = o.EventId AND ct.[__$operation] = 2);
        """;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(Math.Max(5, options.CurrentValue.IntervalSeconds)), clock);
        do
        {
            var current = options.CurrentValue;
            if (!current.Enabled)
            {
                continue;
            }

            try
            {
                var deleted = await PurgeAsync(current, stoppingToken);
                if (deleted > 0)
                {
                    LogPurged(logger, deleted, current.RetentionMinutes);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogPurgeFailed(logger, ex); // retention is best-effort; never crash the core
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    private async Task<int> PurgeAsync(OutboxRetentionOptions current, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<CoreDbContext>();
        var cutoff = clock.GetUtcNow().UtcDateTime.AddMinutes(-current.RetentionMinutes);

        var total = 0;
        int batch;
        do
        {
            batch = await db.Database.ExecuteSqlRawAsync(DeleteBatchSql, [current.BatchSize, cutoff], ct);
            total += batch;
        }
        while (batch == current.BatchSize && !ct.IsCancellationRequested);

        return total;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox retention purged {Deleted} rows older than {RetentionMinutes} min")]
    private static partial void LogPurged(ILogger logger, int deleted, int retentionMinutes);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox retention run failed; will retry next tick")]
    private static partial void LogPurgeFailed(ILogger logger, Exception ex);
}

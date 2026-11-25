using System.Text.Json;
using Makers.Audit.Api.Domain;
using Makers.Audit.Api.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace Makers.Audit.Api.Tests;

public sealed class AuditOutboxInterceptorTests
{
    private static readonly DateTime Now = new(2026, 11, 25, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task Opening_an_account_writes_one_create_event_in_the_same_save()
    {
        await using var db = NewContext();

        var account = Account.Open("ana", 100m, Now);
        db.Accounts.Add(account);
        await db.SaveChangesAsync();

        var evt = Assert.Single(db.AuditOutbox);
        Assert.Equal("Account", evt.EntityType);
        Assert.Equal(account.Id.ToString(), evt.EntityId);
        Assert.Equal("CREATE", evt.Action);
        Assert.Equal("tester", evt.Actor);
        Assert.Equal(AuditOutboxMessage.CurrentSchemaVersion, evt.SchemaVersion);
        Assert.Equal(Now, evt.OccurredAt);
        Assert.NotEqual(Guid.Empty, evt.EventId);
    }

    [Fact]
    public async Task Payment_produces_payment_create_and_account_update_atomically()
    {
        await using var db = NewContext();
        var account = Account.Open("ana", 100m, Now);
        db.Accounts.Add(account);
        await db.SaveChangesAsync();

        db.Payments.Add(Payment.Pay(account, 40m, Now));
        await db.SaveChangesAsync();

        var events = db.AuditOutbox.OrderBy(e => e.EntityType).ThenBy(e => e.Action).ToList();
        Assert.Equal(3, events.Count);
        Assert.Contains(events, e => e is { EntityType: "Payment", Action: "CREATE" });

        var update = Assert.Single(events, e => e is { EntityType: "Account", Action: "UPDATE" });
        using var payload = JsonDocument.Parse(update.Payload!);
        var balance = payload.RootElement.GetProperty("changes").GetProperty("balance");
        Assert.Equal(100m, balance.GetProperty("old").GetDecimal());
        Assert.Equal(60m, balance.GetProperty("new").GetDecimal());
        Assert.Equal(60m, payload.RootElement.GetProperty("amount").GetDecimal());
        Assert.Equal("api", payload.RootElement.GetProperty("channel").GetString());
    }

    [Fact]
    public async Task Ignored_properties_never_reach_the_payload()
    {
        await using var db = NewContext();
        db.Accounts.Add(Account.Open("ana", 10m, Now));
        await db.SaveChangesAsync();

        var payload = Assert.Single(db.AuditOutbox).Payload!;
        Assert.DoesNotContain("rowVersion", payload, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("owner", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Saving_without_auditable_changes_writes_nothing()
    {
        await using var db = NewContext();
        await db.SaveChangesAsync();

        Assert.Empty(db.AuditOutbox);
    }

    [Fact]
    public void Insufficient_funds_is_rejected_by_the_domain()
    {
        var account = Account.Open("ana", 10m, Now);

        Assert.Throws<DomainException>(() => Payment.Pay(account, 11m, Now));
        Assert.Equal(10m, account.Balance);
    }

    private static CoreDbContext NewContext()
    {
        var interceptor = new AuditOutboxInterceptor(new FixedAuditContext(), new FixedClock(Now));
        var options = new DbContextOptionsBuilder<CoreDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .AddInterceptors(interceptor)
            .Options;
        return new CoreDbContext(options);
    }

    private sealed class FixedAuditContext : IAuditContext
    {
        public string Actor => "tester";

        public string Channel => "api";
    }

    private sealed class FixedClock(DateTime utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => new(utcNow);
    }
}

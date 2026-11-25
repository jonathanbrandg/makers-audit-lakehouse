using Makers.Audit.Api.Domain;
using Microsoft.EntityFrameworkCore;

namespace Makers.Audit.Api.Infrastructure;

/// <summary>
/// Schema is owned by infra/sqlserver/init.sql in the PoC (CDC must be enabled on the
/// outbox right after it is created). In a real service: EF migrations + a CDC step.
/// </summary>
public sealed class CoreDbContext(DbContextOptions<CoreDbContext> options) : DbContext(options)
{
    public DbSet<Account> Accounts => Set<Account>();

    public DbSet<Payment> Payments => Set<Payment>();

    public DbSet<AuditOutboxMessage> AuditOutbox => Set<AuditOutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Account>(b =>
        {
            b.ToTable("Accounts");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedOnAdd(); // EF generates sequential GUIDs client-side
            b.Property(x => x.Owner).HasMaxLength(100);
            b.Property(x => x.Balance).HasPrecision(18, 2);
            b.Property(x => x.CreatedAt).HasColumnType("datetime2(3)");
            b.Property(x => x.RowVersion).IsRowVersion(); // optimistic concurrency on balance
            b.Ignore(x => x.AuditAmount);
        });

        modelBuilder.Entity<Payment>(b =>
        {
            b.ToTable("Payments");
            b.HasKey(x => x.Id);
            b.Property(x => x.Id).ValueGeneratedOnAdd();
            b.Property(x => x.Amount).HasPrecision(18, 2);
            b.Property(x => x.Status).HasMaxLength(20);
            b.Property(x => x.CreatedAt).HasColumnType("datetime2(3)");
            b.HasOne<Account>().WithMany().HasForeignKey(x => x.AccountId);
            b.Ignore(x => x.AuditAmount);
        });

        modelBuilder.Entity<AuditOutboxMessage>(b =>
        {
            b.ToTable("AuditOutbox");
            b.HasKey(x => x.EventId);
            b.Property(x => x.EventId).ValueGeneratedOnAdd(); // sequential GUID: no page splits on the clustered PK
            b.Property(x => x.OccurredAt).HasColumnType("datetime2(3)");
            b.Property(x => x.EntityType).HasMaxLength(100);
            b.Property(x => x.EntityId).HasMaxLength(100);
            b.Property(x => x.Action).HasMaxLength(50);
            b.Property(x => x.Actor).HasMaxLength(100);
        });
    }
}

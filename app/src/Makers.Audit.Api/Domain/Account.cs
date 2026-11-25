namespace Makers.Audit.Api.Domain;

public sealed class Account : IAuditableEntity
{
    private Account()
    {
    }

    public Guid Id { get; private set; }

    public string Owner { get; private set; } = string.Empty;

    public decimal Balance { get; private set; }

    public DateTime CreatedAt { get; private set; }

    [AuditIgnore]
    public byte[] RowVersion { get; private set; } = [];

    public decimal? AuditAmount => Balance;

    public static Account Open(string owner, decimal initialBalance, DateTime nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);
        if (initialBalance < 0)
        {
            throw new DomainException("Initial balance cannot be negative.");
        }

        return new Account { Owner = owner.Trim(), Balance = initialBalance, CreatedAt = nowUtc };
    }

    public void Debit(decimal amount)
    {
        if (amount <= 0)
        {
            throw new DomainException("Amount must be positive.");
        }

        if (amount > Balance)
        {
            throw new DomainException("Insufficient funds.");
        }

        Balance -= amount;
    }

    public void Credit(decimal amount)
    {
        if (amount <= 0)
        {
            throw new DomainException("Amount must be positive.");
        }

        Balance += amount;
    }
}

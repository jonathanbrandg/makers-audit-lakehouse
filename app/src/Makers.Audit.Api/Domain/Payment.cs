namespace Makers.Audit.Api.Domain;

public sealed class Payment : IAuditableEntity
{
    public const string Completed = "COMPLETED";
    public const string Reversed = "REVERSED";

    private Payment()
    {
    }

    public Guid Id { get; private set; }

    public Guid AccountId { get; private set; }

    public decimal Amount { get; private set; }

    public string Status { get; private set; } = Completed;

    public DateTime CreatedAt { get; private set; }

    public decimal? AuditAmount => Amount;

    /// <summary>Debits the account and creates the payment (one business transaction).</summary>
    public static Payment Pay(Account account, decimal amount, DateTime nowUtc)
    {
        account.Debit(amount);
        return new Payment { AccountId = account.Id, Amount = amount, Status = Completed, CreatedAt = nowUtc };
    }

    public void Reverse(Account account)
    {
        if (Status == Reversed)
        {
            throw new DomainException("Payment already reversed.");
        }

        if (account.Id != AccountId)
        {
            throw new DomainException("Account does not match the payment.");
        }

        account.Credit(Amount);
        Status = Reversed;
    }
}

public sealed class DomainException(string message) : Exception(message);

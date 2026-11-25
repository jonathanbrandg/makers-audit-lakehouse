using Makers.Audit.Api.Domain;
using Makers.Audit.Api.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace Makers.Audit.Api.Endpoints;

public sealed record OpenAccountRequest(string Owner, decimal InitialBalance);

public sealed record PayRequest(Guid AccountId, decimal Amount);

public sealed record SimulationResult(int Accounts, int Payments, int Reversals, int Conflicts, int Rejected, long ElapsedMs);

public static class CoreEndpoints
{
    public static IEndpointRouteBuilder MapCoreEndpoints(this IEndpointRouteBuilder app)
    {
        // Expected business outcomes are translated here, so they are not logged as
        // unhandled errors by the global exception middleware.
        var api = app.MapGroup("/api").AddEndpointFilter(async (context, next) =>
        {
            try
            {
                return await next(context);
            }
            catch (DomainException ex)
            {
                return Results.Problem(statusCode: StatusCodes.Status422UnprocessableEntity, title: ex.Message);
            }
            catch (DbUpdateConcurrencyException)
            {
                return Results.Problem(statusCode: StatusCodes.Status409Conflict, title: "Concurrent update, retry the operation.");
            }
        });

        api.MapPost("/accounts", async (OpenAccountRequest req, CoreDbContext db, TimeProvider clock, CancellationToken ct) =>
        {
            var account = Account.Open(req.Owner, req.InitialBalance, clock.GetUtcNow().UtcDateTime);
            db.Accounts.Add(account);
            await db.SaveChangesAsync(ct); // account + audit event: one transaction
            return Results.Created($"/api/accounts/{account.Id}", new { account.Id, account.Owner, account.Balance });
        });

        api.MapGet("/accounts/{id:guid}", async (Guid id, CoreDbContext db, CancellationToken ct) =>
            await db.Accounts.AsNoTracking().FirstOrDefaultAsync(a => a.Id == id, ct) is { } a
                ? Results.Ok(new { a.Id, a.Owner, a.Balance, a.CreatedAt })
                : Results.NotFound());

        api.MapPost("/payments", async (PayRequest req, CoreDbContext db, TimeProvider clock, CancellationToken ct) =>
        {
            var account = await db.Accounts.FirstOrDefaultAsync(a => a.Id == req.AccountId, ct);
            if (account is null)
            {
                return Results.NotFound();
            }

            var payment = Payment.Pay(account, req.Amount, clock.GetUtcNow().UtcDateTime);
            db.Payments.Add(payment);
            await db.SaveChangesAsync(ct); // payment + balance + 2 audit events: atomic
            return Results.Created($"/api/payments/{payment.Id}", new { payment.Id, payment.Status, account.Balance });
        });

        api.MapPost("/payments/{id:guid}/reverse", async (Guid id, CoreDbContext db, CancellationToken ct) =>
        {
            var payment = await db.Payments.FirstOrDefaultAsync(p => p.Id == id, ct);
            if (payment is null)
            {
                return Results.NotFound();
            }

            var account = await db.Accounts.FirstAsync(a => a.Id == payment.AccountId, ct);
            payment.Reverse(account);
            await db.SaveChangesAsync(ct);
            return Results.Ok(new { payment.Id, payment.Status, account.Balance });
        });

        // Peak-load simulator: N independent business transactions through the real code path.
        api.MapPost("/simulate", async (int? accounts, int? payments, int? parallelism, IServiceScopeFactory scopes, CancellationToken ct) =>
            Results.Ok(await Simulate(scopes, Math.Clamp(accounts ?? 20, 1, 1_000), Math.Clamp(payments ?? 500, 0, 100_000), Math.Clamp(parallelism ?? 8, 1, 32), ct)));

        return app;
    }

    private static async Task<SimulationResult> Simulate(IServiceScopeFactory scopes, int accountCount, int paymentCount, int parallelism, CancellationToken ct)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        var accountIds = new List<Guid>(accountCount);
        for (var i = 0; i < accountCount; i++)
        {
            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<CoreDbContext>();
            var clock = scope.ServiceProvider.GetRequiredService<TimeProvider>();
            var account = Account.Open($"customer-{Random.Shared.Next(100_000)}", 1_000_000m, clock.GetUtcNow().UtcDateTime);
            db.Accounts.Add(account);
            await db.SaveChangesAsync(ct);
            accountIds.Add(account.Id);
        }

        int paid = 0, reversed = 0, conflicts = 0, rejected = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(0, paymentCount),
            new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = ct },
            async (_, token) =>
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<CoreDbContext>();
                var clock = scope.ServiceProvider.GetRequiredService<TimeProvider>();
                try
                {
                    var accountId = accountIds[Random.Shared.Next(accountIds.Count)];
                    var account = await db.Accounts.FirstAsync(a => a.Id == accountId, token);
                    var payment = Payment.Pay(account, Random.Shared.Next(1, 5_000), clock.GetUtcNow().UtcDateTime);
                    db.Payments.Add(payment);
                    await db.SaveChangesAsync(token);
                    Interlocked.Increment(ref paid);

                    if (Random.Shared.Next(10) == 0)
                    {
                        payment.Reverse(account);
                        await db.SaveChangesAsync(token);
                        Interlocked.Increment(ref reversed);
                    }
                }
                catch (DbUpdateConcurrencyException)
                {
                    Interlocked.Increment(ref conflicts); // two payments on the same balance: rowversion wins
                }
                catch (DomainException)
                {
                    Interlocked.Increment(ref rejected);
                }
            });

        return new SimulationResult(accountCount, paid, reversed, conflicts, rejected, started.ElapsedMilliseconds);
    }
}

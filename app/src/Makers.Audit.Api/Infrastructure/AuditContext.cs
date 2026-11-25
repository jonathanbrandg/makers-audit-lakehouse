namespace Makers.Audit.Api.Infrastructure;

/// <summary>Who is acting and through which channel (stored in every audit event).</summary>
public interface IAuditContext
{
    string Actor { get; }

    string Channel { get; }
}

/// <summary>
/// PoC: actor/channel come from X-Actor / X-Channel headers. In production the actor
/// comes from the authenticated principal (claims), never from a client header.
/// </summary>
public sealed class HttpAuditContext(IHttpContextAccessor accessor) : IAuditContext
{
    public const string SystemActor = "system";

    public string Actor => Header("X-Actor") ?? SystemActor;

    public string Channel => Header("X-Channel") ?? (accessor.HttpContext is null ? "worker" : "api");

    private string? Header(string name)
    {
        var value = accessor.HttpContext?.Request.Headers[name].ToString();
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim()[..Math.Min(value.Trim().Length, 100)];
    }
}

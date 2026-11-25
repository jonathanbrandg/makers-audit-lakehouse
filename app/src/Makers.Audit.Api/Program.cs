using Makers.Audit.Api.Endpoints;
using Makers.Audit.Api.Infrastructure;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddHttpContextAccessor();
builder.Services.AddSingleton<IAuditContext, HttpAuditContext>();
builder.Services.AddSingleton<AuditOutboxInterceptor>();

builder.Services.AddDbContext<CoreDbContext>((sp, options) => options
    .UseSqlServer(
        builder.Configuration.GetConnectionString("Core"),
        sql => sql.EnableRetryOnFailure(maxRetryCount: 5)) // safe: one SaveChanges per transaction
    .AddInterceptors(sp.GetRequiredService<AuditOutboxInterceptor>()));

builder.Services.Configure<OutboxRetentionOptions>(builder.Configuration.GetSection(OutboxRetentionOptions.Section));
builder.Services.AddHostedService<OutboxRetentionService>();

builder.Services.AddHealthChecks().AddDbContextCheck<CoreDbContext>();
builder.Services.AddProblemDetails();

var app = builder.Build();

app.UseExceptionHandler(errors => errors.Run(async context =>
{
    var error = context.Features.Get<IExceptionHandlerFeature>()?.Error;
    var (status, title) = error switch
    {
        BadHttpRequestException => (StatusCodes.Status400BadRequest, "Invalid request."),
        _ => (StatusCodes.Status500InternalServerError, "Unexpected error."),
    };
    await Results.Problem(statusCode: status, title: title).ExecuteAsync(context);
}));

app.MapHealthChecks("/health");
app.MapCoreEndpoints();

app.Run();

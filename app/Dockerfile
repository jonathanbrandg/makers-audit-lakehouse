FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/Makers.Audit.Api/Makers.Audit.Api.csproj src/Makers.Audit.Api/
COPY tests/Makers.Audit.Api.Tests/Makers.Audit.Api.Tests.csproj tests/Makers.Audit.Api.Tests/
RUN dotnet restore tests/Makers.Audit.Api.Tests/Makers.Audit.Api.Tests.csproj
COPY . .
# Tests gate the image: a broken outbox contract never ships.
RUN dotnet test tests/Makers.Audit.Api.Tests/Makers.Audit.Api.Tests.csproj -c Release --no-restore --logger "console;verbosity=normal"
RUN dotnet publish src/Makers.Audit.Api/Makers.Audit.Api.csproj -c Release --no-restore -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
USER $APP_UID
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Makers.Audit.Api.dll"]

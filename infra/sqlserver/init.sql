-- Origen transaccional: tabla outbox de auditoría escrita por el core .NET
-- en la MISMA transacción que la operación de negocio (sin dual-write).
-- CDC lee el transaction log de forma asíncrona: no agrega locks al camino OLTP.

IF DB_ID(N'AuditDb') IS NULL
    CREATE DATABASE AuditDb;
GO

USE AuditDb;
GO

IF OBJECT_ID(N'dbo.AuditOutbox', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.AuditOutbox
    (
        EventId       UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_AuditOutbox_EventId DEFAULT NEWSEQUENTIALID(),
        OccurredAt    DATETIME2(3)     NOT NULL CONSTRAINT DF_AuditOutbox_OccurredAt DEFAULT SYSUTCDATETIME(),
        EntityType    NVARCHAR(100)    NOT NULL,
        EntityId      NVARCHAR(100)    NOT NULL,
        Action        NVARCHAR(50)     NOT NULL,
        Actor         NVARCHAR(100)    NOT NULL,
        SchemaVersion INT              NOT NULL CONSTRAINT DF_AuditOutbox_SchemaVersion DEFAULT 1,
        Payload       NVARCHAR(MAX)    NULL,
        CONSTRAINT PK_AuditOutbox PRIMARY KEY CLUSTERED (EventId)
    );
END
GO

-- Tablas de negocio del core .NET (app/). CDC NO se habilita sobre ellas: el único
-- contrato con el lakehouse es la outbox, así el esquema interno evoluciona libre.
IF OBJECT_ID(N'dbo.Accounts', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Accounts
    (
        Id         UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Accounts PRIMARY KEY CLUSTERED,
        Owner      NVARCHAR(100)    NOT NULL,
        Balance    DECIMAL(18,2)    NOT NULL,
        CreatedAt  DATETIME2(3)     NOT NULL,
        RowVersion ROWVERSION       NOT NULL
    );
END
GO

IF OBJECT_ID(N'dbo.Payments', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Payments
    (
        Id         UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Payments PRIMARY KEY CLUSTERED,
        AccountId  UNIQUEIDENTIFIER NOT NULL CONSTRAINT FK_Payments_Accounts REFERENCES dbo.Accounts (Id),
        Amount     DECIMAL(18,2)    NOT NULL,
        Status     NVARCHAR(20)     NOT NULL,
        CreatedAt  DATETIME2(3)     NOT NULL
    );
    CREATE INDEX IX_Payments_AccountId ON dbo.Payments (AccountId);
END
GO

-- Soporta el borrado por lotes de la retención OLTP (OutboxRetentionService).
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_AuditOutbox_OccurredAt')
    CREATE INDEX IX_AuditOutbox_OccurredAt ON dbo.AuditOutbox (OccurredAt);
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'AuditDb' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables ct
    JOIN sys.tables t ON t.object_id = ct.source_object_id
    WHERE t.name = N'AuditOutbox')
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'AuditOutbox',
        @role_name = NULL,
        @supports_net_changes = 0;
END
GO

-- Semilla: estas filas llegan como snapshot inicial (op = 'r') de Debezium.
IF NOT EXISTS (SELECT 1 FROM dbo.AuditOutbox)
BEGIN
    INSERT INTO dbo.AuditOutbox (EntityType, EntityId, Action, Actor, Payload)
    SELECT TOP (1000)
        CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN N'Account' WHEN 1 THEN N'Payment' ELSE N'Customer' END,
        CONCAT(N'E-', ABS(CHECKSUM(NEWID())) % 5000),
        CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN N'CREATE' WHEN 1 THEN N'UPDATE' ELSE N'DELETE' END,
        CONCAT(N'user-', ABS(CHECKSUM(NEWID())) % 50),
        CONCAT(N'{"amount":', ABS(CHECKSUM(NEWID())) % 100000, N',"channel":"seed"}')
    FROM sys.all_objects a CROSS JOIN sys.all_objects b;
END
GO

PRINT 'AuditDb listo (outbox + CDC).';
GO

/* ============================================================
   MechmanRetail BI Portfolio Project
   Phase 1: SQL Server — Complete Implementation
   Author: Adiga Triumphant

   THREE-TIER ARCHITECTURE
   ─────────────────────────────────────────────────────────
   MechmanRetail_SOURCE
     Full raw dataset — all years. Never touched analytically.
     Represents the operational source system.
     Restored from the full Wide World Importers backup.

   MechmanRetail_DEV
     Staging environment. Holds only the latest unprocessed
     month at any one time. Truncated and reloaded each cycle
     by sp_LoadLatestMonthToDEV. Validated before promotion.

   MechmanRetail_PROD
     Full historical analytical dataset. Grows incrementally
     as each new month is promoted from DEV.
     Power BI published report connects here.

   PIPELINE FLOW
   ─────────────────────────────────────────────────────────
   SOURCE → sp_LoadLatestMonthToDEV → DEV
          → sp_PromoteDEVtoPROD     → PROD
          → Power BI refresh

   COLUMN STANDARDISATION
   ─────────────────────────────────────────────────────────
   dbo.Sale: source table — unchanged. Original spaced names.
   vw_Sales_Base: 12 columns only — clean camelCase aliases.
     WITH SCHEMABINDING — required by RLS security policy.
     This is the ONLY standardisation point in the project.
     Power BI connects here — never to dbo.Sale directly.

   Excluded from vw_Sales_Base (not used in Power BI model):
     [WWI Invoice ID], [Unit Price], [Tax Rate],
     [Tax Amount], [Total Including Tax]

   EXECUTION ORDER
   ─────────────────────────────────────────────────────────
   1. Primary key and indexes     — DEV then PROD
   2. Create views                — DEV then PROD
   3. Create stored procedures    — DEV then PROD
   4. RLS users, grants, policy   — PROD only
   5. Verify                      — DEV then PROD
   ============================================================ */


/* ============================================================
   SECTION 1 — PRIMARY KEY AND INDEXES
   Run against MechmanRetail_DEV first, then MechmanRetail_PROD
   ============================================================ */

USE MechmanRetail_DEV;
GO

-- Make [Sale Key] non-nullable before adding primary key
ALTER TABLE dbo.Sale
    ALTER COLUMN [Sale Key] INT NOT NULL;
GO

ALTER TABLE dbo.Sale
    ADD CONSTRAINT PK_Sale PRIMARY KEY CLUSTERED ([Sale Key]);
GO

CREATE NONCLUSTERED INDEX IX_Sale_InvoiceDateKey
    ON dbo.Sale ([Invoice Date Key]);

CREATE NONCLUSTERED INDEX IX_Sale_CustomerKey
    ON dbo.Sale ([Customer Key]);

CREATE NONCLUSTERED INDEX IX_Sale_SalespersonKey
    ON dbo.Sale ([Salesperson Key]);
GO

-- Verify
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Sale'
ORDER BY ORDINAL_POSITION;

SELECT COUNT(*) AS RowCount FROM dbo.Sale;
GO


/* ============================================================
   SECTION 2 — VIEWS
   Run against MechmanRetail_DEV first, then MechmanRetail_PROD
   ============================================================ */

-- ─────────────────────────────────────────────────────────────
-- 2A. vw_Sales_Base
-- Primary Power BI connection. 12 columns only.
-- WITH SCHEMABINDING required by the RLS security policy.
-- Exposes only columns actively consumed by the Power BI model.
-- Clean camelCase aliases — the single standardisation point.
-- Five unused columns excluded: [WWI Invoice ID], [Unit Price],
-- [Tax Rate], [Tax Amount], [Total Including Tax].
-- ─────────────────────────────────────────────────────────────
CREATE VIEW dbo.vw_Sales_Base
WITH SCHEMABINDING
AS
SELECT
    [Sale Key]            AS SaleKey,
    [City Key]            AS CityKey,
    [Customer Key]        AS CustomerKey,
    [Stock Item Key]      AS StockItemKey,
    [Invoice Date Key]    AS InvoiceDateKey,
    [Delivery Date Key]   AS DeliveryDateKey,
    [Salesperson Key]     AS SalespersonKey,
    Quantity              AS Quantity,
    [Total Excluding Tax] AS TotalExcludingTax,
    Profit                AS Profit,
    [Total Chiller Items] AS TotalChillerItems,
    [Total Dry Items]     AS TotalDryItems
FROM dbo.Sale;
GO


-- ─────────────────────────────────────────────────────────────
-- 2B. vw_Salesperson_KPI
-- Annual salesperson rankings via RANK() window function.
-- NOT consumed by Power BI — salesperson rankings use a RANKX
-- DAX measure that responds dynamically to filter context.
-- Retained as a database-layer endpoint for direct SQL access.
-- ─────────────────────────────────────────────────────────────
CREATE VIEW dbo.vw_Salesperson_KPI AS
SELECT
    [Salesperson Key]                       AS SalespersonKey,
    YEAR([Invoice Date Key])                AS InvoiceYear,
    COUNT(DISTINCT [Sale Key])              AS TransactionCount,
    SUM([Total Excluding Tax])              AS TotalRevenue,
    SUM(Profit)                             AS TotalProfit,
    CAST(
        SUM(Profit) * 100.0 /
        NULLIF(SUM([Total Excluding Tax]),0)
    AS DECIMAL(5,2))                        AS ProfitMarginPct,
    RANK() OVER (
        PARTITION BY YEAR([Invoice Date Key])
        ORDER BY SUM([Total Excluding Tax]) DESC
    )                                       AS RevenueRankByYear
FROM dbo.Sale
GROUP BY
    [Salesperson Key],
    YEAR([Invoice Date Key]);
GO


-- ─────────────────────────────────────────────────────────────
-- 2C. vw_Territory_Sales
-- RLS bridge view — CityKey to territory linkage.
-- NOT active. Retained alongside the inactive RLS policy.
-- Power BI Import mode handles RLS at the Power BI layer.
-- ─────────────────────────────────────────────────────────────
CREATE VIEW dbo.vw_Territory_Sales AS
SELECT DISTINCT
    [City Key]        AS CityKey,
    [Salesperson Key] AS SalespersonKey,
    [Customer Key]    AS CustomerKey
FROM dbo.Sale;
GO

-- Verify all views
SELECT 'vw_Sales_Base'      AS ViewName, COUNT(*) AS Rows FROM dbo.vw_Sales_Base;
SELECT 'vw_Salesperson_KPI' AS ViewName, COUNT(*) AS Rows FROM dbo.vw_Salesperson_KPI;
SELECT 'vw_Territory_Sales' AS ViewName, COUNT(*) AS Rows FROM dbo.vw_Territory_Sales;
GO


/* ============================================================
   SECTION 3 — STORED PROCEDURES
   Run against MechmanRetail_DEV
   ============================================================ */

USE MechmanRetail_DEV;
GO

-- ─────────────────────────────────────────────────────────────
-- 3A. sp_LoadLatestMonthToDEV
-- Entry point of the pipeline. Run from MechmanRetail_DEV.
-- Identifies the latest month in MechmanRetail_SOURCE that
-- does not yet exist in MechmanRetail_PROD and loads it
-- into MechmanRetail_DEV for staging and validation.
-- DEV is truncated before each load — holds one month only.
--
-- Detection logic:
--   Compares SOURCE against PROD at month-level granularity.
--   DEV is a temporary staging area — not used for comparison.
--
-- Execution flow:
--   Step 1: Find latest month in SOURCE not present in PROD
--   Step 2: Exit cleanly if SOURCE has nothing new
--   Step 3: Truncate DEV — clear previous staging data
--   Step 4: Load the new month from SOURCE into DEV
--   Step 5: Return load summary
-- ─────────────────────────────────────────────────────────────
CREATE PROCEDURE dbo.sp_LoadLatestMonthToDEV AS
BEGIN
    SET NOCOUNT ON;

    -- ─────────────────────────────────────────────────────
    -- STEP 1: Find latest month in SOURCE not present in PROD
    -- ─────────────────────────────────────────────────────
    DECLARE @TargetYear  INT;
    DECLARE @TargetMonth INT;

    SELECT TOP 1
        @TargetYear  = YEAR(s.[Invoice Date Key]),
        @TargetMonth = MONTH(s.[Invoice Date Key])
    FROM MechmanRetail_SOURCE.dbo.Sale s
    WHERE NOT EXISTS (
        SELECT 1
        FROM MechmanRetail_PROD.dbo.Sale p
        WHERE YEAR(p.[Invoice Date Key])  = YEAR(s.[Invoice Date Key])
          AND MONTH(p.[Invoice Date Key]) = MONTH(s.[Invoice Date Key])
    )
    ORDER BY
        YEAR(s.[Invoice Date Key])  DESC,
        MONTH(s.[Invoice Date Key]) DESC;

    -- ─────────────────────────────────────────────────────
    -- STEP 2: Exit cleanly if SOURCE has nothing new
    -- ─────────────────────────────────────────────────────
    IF @TargetYear IS NULL
    BEGIN
        PRINT 'No new months found in SOURCE. PROD is already up to date.';
        RETURN;
    END

    PRINT 'New month detected: '
        + CAST(@TargetYear AS VARCHAR(4))
        + '-'
        + RIGHT('0' + CAST(@TargetMonth AS VARCHAR(2)), 2);

    -- ─────────────────────────────────────────────────────
    -- STEP 3: Truncate DEV — clear previous staging data
    -- ─────────────────────────────────────────────────────
    TRUNCATE TABLE MechmanRetail_DEV.dbo.Sale;

    -- ─────────────────────────────────────────────────────
    -- STEP 4: Load the new month from SOURCE into DEV
    -- ─────────────────────────────────────────────────────
    INSERT INTO MechmanRetail_DEV.dbo.Sale
    SELECT *
    FROM MechmanRetail_SOURCE.dbo.Sale
    WHERE YEAR([Invoice Date Key])  = @TargetYear
      AND MONTH([Invoice Date Key]) = @TargetMonth;

    DECLARE @RowsLoaded INT = @@ROWCOUNT;

    -- ─────────────────────────────────────────────────────
    -- STEP 5: Return load summary
    -- ─────────────────────────────────────────────────────
    SELECT
        'SUCCESS'                                     AS Status,
        @TargetYear                                   AS Staged_Year,
        @TargetMonth                                  AS Staged_Month,
        @RowsLoaded                                   AS Rows_Loaded_To_DEV,
        'Run sp_PromoteDEVtoPROD to promote to PROD.' AS Next_Step;

END;
GO


-- ─────────────────────────────────────────────────────────────
-- 3B. sp_EnvironmentHealthCheck
-- Read-only validation — no data is moved or changed.
-- Run against any database to validate its current state.
-- Returns two result sets:
--   1. Detailed diagnostic row — counts, dates, totals
--   2. PASS/FAIL summary row — one result per check
-- Run manually in SSMS before and after pipeline execution
-- to inspect database state. Not called internally by
-- sp_PromoteDEVtoPROD — inline validation is used there
-- to avoid multi-result-set capture complications.
-- ─────────────────────────────────────────────────────────────
CREATE PROCEDURE dbo.sp_EnvironmentHealthCheck AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowCount  INT;
    DECLARE @MinDate   DATE;
    DECLARE @MaxDate   DATE;
    DECLARE @NullCust  INT;
    DECLARE @NullProd  INT;
    DECLARE @NullSales INT;
    DECLARE @NullCity  INT;
    DECLARE @Revenue   DECIMAL(18,2);
    DECLARE @Profit    DECIMAL(18,2);
    DECLARE @DupKeys   INT;

    SELECT
        @RowCount  = COUNT(*),
        @MinDate   = MIN([Invoice Date Key]),
        @MaxDate   = MAX([Invoice Date Key]),
        @NullCust  = SUM(CASE WHEN [Customer Key]    IS NULL THEN 1 ELSE 0 END),
        @NullProd  = SUM(CASE WHEN [Stock Item Key]  IS NULL THEN 1 ELSE 0 END),
        @NullSales = SUM(CASE WHEN [Salesperson Key] IS NULL THEN 1 ELSE 0 END),
        @NullCity  = SUM(CASE WHEN [City Key]        IS NULL THEN 1 ELSE 0 END),
        @Revenue   = SUM([Total Excluding Tax]),
        @Profit    = SUM(Profit)
    FROM dbo.Sale;

    SELECT @DupKeys = COUNT(*) FROM (
        SELECT [Sale Key]
        FROM dbo.Sale
        GROUP BY [Sale Key]
        HAVING COUNT(*) > 1
    ) d;

    -- Result set 1: Detailed diagnostic row
    SELECT
        DB_NAME()  AS Database_Name,
        @RowCount  AS Row_Count,
        @MinDate   AS Earliest_Date,
        @MaxDate   AS Latest_Date,
        @NullCust  AS Null_CustomerKeys,
        @NullProd  AS Null_StockItemKeys,
        @NullSales AS Null_SalespersonKeys,
        @NullCity  AS Null_CityKeys,
        @Revenue   AS Total_Revenue,
        @Profit    AS Total_Profit,
        @DupKeys   AS Duplicate_SaleKeys;

    -- Result set 2: PASS/FAIL summary
    SELECT
        CASE WHEN @NullCust  = 0 THEN 'PASS' ELSE 'FAIL' END AS CustomerKey_Nulls,
        CASE WHEN @NullProd  = 0 THEN 'PASS' ELSE 'FAIL' END AS StockItemKey_Nulls,
        CASE WHEN @NullSales = 0 THEN 'PASS' ELSE 'FAIL' END AS SalespersonKey_Nulls,
        CASE WHEN @NullCity  = 0 THEN 'PASS' ELSE 'FAIL' END AS CityKey_Nulls,
        CASE WHEN @DupKeys   = 0 THEN 'PASS' ELSE 'FAIL' END AS Duplicate_SaleKeys,
        CASE WHEN @RowCount  > 0 THEN 'PASS' ELSE 'FAIL' END AS Has_Rows;
END;
GO


-- ─────────────────────────────────────────────────────────────
-- 3C. sp_PromoteDEVtoPROD
-- Run from MechmanRetail_DEV only.
-- Incremental promotion — detects new months in DEV that are
-- not yet present in PROD at month-level granularity.
-- Promotes only new months — historical PROD data is preserved.
-- No manual checking required — detection is fully automated.
-- Uses inline validation rather than calling
-- sp_EnvironmentHealthCheck to avoid multi-result-set
-- capture complications with INSERT EXEC.
--
-- Execution flow:
--   Step 1: Inline health check DEV — abort if any check fails
--   Step 2: Detect new months in DEV not present in PROD
--   Step 3: Exit cleanly if no new months found
--   Step 4: Promote new months inside a transaction
--   Step 5: Validate PROD row count increased correctly
--   Step 6: Inline health check PROD — alert if check fails
--   Step 7: Return success report
-- ─────────────────────────────────────────────────────────────
CREATE PROCEDURE dbo.sp_PromoteDEVtoPROD AS
BEGIN
    SET NOCOUNT ON;

    -- ─────────────────────────────────────────────────────
    -- STEP 1: Inline health check on DEV before touching PROD
    -- ─────────────────────────────────────────────────────
    DECLARE @NullCust  INT;
    DECLARE @NullProd  INT;
    DECLARE @NullSales INT;
    DECLARE @NullCity  INT;
    DECLARE @DupKeys   INT;
    DECLARE @RowCount  INT;

    SELECT
        @RowCount  = COUNT(*),
        @NullCust  = SUM(CASE WHEN [Customer Key]    IS NULL THEN 1 ELSE 0 END),
        @NullProd  = SUM(CASE WHEN [Stock Item Key]  IS NULL THEN 1 ELSE 0 END),
        @NullSales = SUM(CASE WHEN [Salesperson Key] IS NULL THEN 1 ELSE 0 END),
        @NullCity  = SUM(CASE WHEN [City Key]        IS NULL THEN 1 ELSE 0 END)
    FROM dbo.Sale;

    SELECT @DupKeys = COUNT(*) FROM (
        SELECT [Sale Key]
        FROM dbo.Sale
        GROUP BY [Sale Key]
        HAVING COUNT(*) > 1
    ) d;

    IF @NullCust > 0 OR @NullProd > 0 OR @NullSales > 0
        OR @NullCity > 0 OR @DupKeys > 0 OR @RowCount = 0
    BEGIN
        RAISERROR(
            'DEV health check failed. Promotion aborted. Review sp_EnvironmentHealthCheck.',
            16, 1
        );
        RETURN;
    END

    -- ─────────────────────────────────────────────────────
    -- STEP 2: Detect new months in DEV not present in PROD
    -- Comparison at month-level granularity.
    -- A month is new if no rows exist in PROD for that
    -- year-month combination.
    -- ─────────────────────────────────────────────────────
    CREATE TABLE #NewMonths (
        InvoiceYear  INT,
        InvoiceMonth INT
    );

    INSERT INTO #NewMonths (InvoiceYear, InvoiceMonth)
    SELECT DISTINCT
        YEAR(d.[Invoice Date Key]),
        MONTH(d.[Invoice Date Key])
    FROM MechmanRetail_DEV.dbo.Sale d
    WHERE NOT EXISTS (
        SELECT 1
        FROM MechmanRetail_PROD.dbo.Sale p
        WHERE YEAR(p.[Invoice Date Key])  = YEAR(d.[Invoice Date Key])
          AND MONTH(p.[Invoice Date Key]) = MONTH(d.[Invoice Date Key])
    );

    -- ─────────────────────────────────────────────────────
    -- STEP 3: Exit cleanly if no new months found
    -- ─────────────────────────────────────────────────────
    IF NOT EXISTS (SELECT 1 FROM #NewMonths)
    BEGIN
        DROP TABLE #NewMonths;
        PRINT 'No new months detected in DEV. PROD is already up to date.';
        RETURN;
    END

    -- Report which months will be promoted
    SELECT
        InvoiceYear,
        InvoiceMonth,
        'Pending promotion' AS Status
    FROM #NewMonths
    ORDER BY InvoiceYear, InvoiceMonth;

    -- ─────────────────────────────────────────────────────
    -- STEP 4: Promote new months inside a transaction
    -- ─────────────────────────────────────────────────────
    DECLARE @PRODRowsBefore  INT;
    DECLARE @PRODRowsAfter   INT;
    DECLARE @NewRowsInserted INT;
    DECLARE @ExpectedRows    INT;

    SELECT @PRODRowsBefore = COUNT(*)
    FROM MechmanRetail_PROD.dbo.Sale;

    BEGIN TRANSACTION;
    BEGIN TRY

        INSERT INTO MechmanRetail_PROD.dbo.Sale
        SELECT d.*
        FROM MechmanRetail_DEV.dbo.Sale d
        INNER JOIN #NewMonths nm
            ON  YEAR(d.[Invoice Date Key])  = nm.InvoiceYear
            AND MONTH(d.[Invoice Date Key]) = nm.InvoiceMonth;

        SET @NewRowsInserted = @@ROWCOUNT;

        SELECT @PRODRowsAfter = COUNT(*)
        FROM MechmanRetail_PROD.dbo.Sale;

        -- ─────────────────────────────────────────────────
        -- STEP 5: Validate PROD row count increased correctly
        -- ─────────────────────────────────────────────────
        SET @ExpectedRows = @PRODRowsBefore + @NewRowsInserted;

        IF @PRODRowsAfter <> @ExpectedRows
        BEGIN
            ROLLBACK TRANSACTION;
            DROP TABLE #NewMonths;
            RAISERROR(
                'Row count validation failed. Expected: %d  Actual: %d. Rolled back.',
                16, 1,
                @ExpectedRows,
                @PRODRowsAfter
            );
            RETURN;
        END

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        DROP TABLE #NewMonths;
        THROW;
    END CATCH;

    -- ─────────────────────────────────────────────────────
    -- STEP 6: Inline health check on PROD after promotion
    -- ─────────────────────────────────────────────────────
    DECLARE @PNullCust  INT;
    DECLARE @PNullProd  INT;
    DECLARE @PNullSales INT;
    DECLARE @PNullCity  INT;
    DECLARE @PDupKeys   INT;
    DECLARE @PRowCount  INT;

    SELECT
        @PRowCount  = COUNT(*),
        @PNullCust  = SUM(CASE WHEN [Customer Key]    IS NULL THEN 1 ELSE 0 END),
        @PNullProd  = SUM(CASE WHEN [Stock Item Key]  IS NULL THEN 1 ELSE 0 END),
        @PNullSales = SUM(CASE WHEN [Salesperson Key] IS NULL THEN 1 ELSE 0 END),
        @PNullCity  = SUM(CASE WHEN [City Key]        IS NULL THEN 1 ELSE 0 END)
    FROM MechmanRetail_PROD.dbo.Sale;

    SELECT @PDupKeys = COUNT(*) FROM (
        SELECT [Sale Key]
        FROM MechmanRetail_PROD.dbo.Sale
        GROUP BY [Sale Key]
        HAVING COUNT(*) > 1
    ) d;

    IF @PNullCust > 0 OR @PNullProd > 0 OR @PNullSales > 0
        OR @PNullCity > 0 OR @PDupKeys > 0 OR @PRowCount = 0
    BEGIN
        DROP TABLE #NewMonths;
        RAISERROR(
            'PROD health check failed after promotion. Investigate immediately.',
            16, 1
        );
        RETURN;
    END

    DROP TABLE #NewMonths;

    -- ─────────────────────────────────────────────────────
    -- STEP 7: Final success report
    -- ─────────────────────────────────────────────────────
    SELECT
        'SUCCESS'                         AS Status,
        @PRODRowsBefore                   AS PROD_Rows_Before,
        @NewRowsInserted                  AS New_Rows_Inserted,
        @PRODRowsAfter                    AS PROD_Rows_After,
        'Incremental promotion complete.' AS Message;

END;
GO


/* ============================================================
   SECTION 4 — ROW LEVEL SECURITY
   Run against MechmanRetail_PROD only.
   STATE = OFF — inactive by design.
   Power BI Import mode handles RLS at the Power BI layer.
   ============================================================ */

USE MechmanRetail_PROD;
GO

-- 4A. Database users WITHOUT LOGIN — RLS test principals
-- Created with IF NOT EXISTS to make the script re-runnable
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'BI_Admin')
    CREATE USER BI_Admin WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'Southeast_Manager')
    CREATE USER Southeast_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'Mideast_Manager')
    CREATE USER Mideast_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'Southwest_Manager')
    CREATE USER Southwest_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'Plains_Manager')
    CREATE USER Plains_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'GreatLakes_Manager')
    CREATE USER GreatLakes_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'FarWest_Manager')
    CREATE USER FarWest_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'RockyMountain_Manager')
    CREATE USER RockyMountain_Manager WITHOUT LOGIN;
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'NewEngland_Manager')
    CREATE USER NewEngland_Manager WITHOUT LOGIN;
GO

-- 4B. Grant SELECT on vw_Sales_Base only
-- vw_Salesperson_KPI and vw_Territory_Sales are not
-- consumer-facing objects — no grants applied to them
GRANT SELECT ON dbo.vw_Sales_Base TO BI_Admin;
GRANT SELECT ON dbo.vw_Sales_Base TO Southeast_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO Mideast_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO Southwest_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO Plains_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO GreatLakes_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO FarWest_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO RockyMountain_Manager;
GRANT SELECT ON dbo.vw_Sales_Base TO NewEngland_Manager;
GO

-- 4C. Predicate function — WITH SCHEMABINDING, admin bypass
IF OBJECT_ID('dbo.fn_SalesTerritoryPredicate', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_SalesTerritoryPredicate;
GO

CREATE FUNCTION dbo.fn_SalesTerritoryPredicate(@CityKey INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS fn_result
    WHERE
        USER_NAME() IN ('BI_Admin', 'dbo')
        OR IS_MEMBER('db_owner') = 1
        OR 1 = 1; -- Placeholder: replace with real territory CityKey mappings
GO

-- 4D. Security policy — STATE = OFF intentionally
-- Applied to vw_Sales_Base — the only consumer-facing view.
-- vw_Sales_Base uses WITH SCHEMABINDING satisfying the
-- schema binding requirement of the security policy.
CREATE SECURITY POLICY dbo.SalesTerritoryPolicy
ADD FILTER PREDICATE dbo.fn_SalesTerritoryPredicate(CityKey)
ON dbo.vw_Sales_Base
WITH (STATE = OFF);
GO

-- To activate in future if switching to DirectQuery:
-- ALTER SECURITY POLICY dbo.SalesTerritoryPolicy WITH (STATE = ON);

PRINT 'RLS users, grants, predicate function and policy created.';
GO


/* ============================================================
   SECTION 5 — FINAL VERIFICATION
   Run against both DEV and PROD
   ============================================================ */

USE MechmanRetail_DEV;
GO
EXEC dbo.sp_EnvironmentHealthCheck;
GO

USE MechmanRetail_PROD;
GO
EXEC dbo.sp_EnvironmentHealthCheck;
GO

/*
   EXPECTED PROD RESULTS
   ─────────────────────
   Row_Count           228,265
   Earliest_Date       2017-05-23
   Latest_Date         2020-10-22
   All null checks     PASS
   Duplicate_SaleKeys  PASS

   EXPECTED DEV RESULTS (after sp_LoadLatestMonthToDEV)
   ─────────────────────────────────────────────────────
   Row_Count           Latest staged month rows only
   Earliest_Date       First day of latest staged month
   Latest_Date         Last day of latest staged month
   All null checks     PASS
   Duplicate_SaleKeys  PASS

   PIPELINE EXECUTION (ongoing — run in this order)
   ─────────────────────────────────────────────────
   USE MechmanRetail_DEV;
   EXEC dbo.sp_LoadLatestMonthToDEV;   -- Stage new month from SOURCE
   EXEC dbo.sp_PromoteDEVtoPROD;       -- Validate and promote to PROD
*/

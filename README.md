# MechmanRetail BI Portfolio Project

A four-page retail sales Business Intelligence report built on the Wide World Importers dataset covering 228,265 transactions across May 2017 to October 2020 — $172M revenue, 49.8% average profit margin.

## Live Report
> Interactive report available on request.

[View PDF Report](reports/Mechman.pdf)

**Power BI Desktop file available on request.**
Contact me via [LinkedIn]([your LinkedIn URL here]) to receive the .pbix file directly.

## Project Architecture

| Phase | Tool | Scope |
|-------|------|-------|
| Phase 1 | SQL Server | Three-tier SOURCE/DEV/PROD incremental pipeline, views, stored procedures, RLS |
| Phase 2 | Power Query | Star schema transformation, data cleaning, 9 queries |
| Phase 3 | Power BI Desktop | 25 standalone DAX measures, field parameter, four-page report |
| Phase 4 | Power BI Service + Novypro | Publishing, RLS, portfolio deployment |

## Report Pages

| Page | Description |
|------|-------------|
| Executive Summary | Annual revenue vs target, monthly trend, territory treemap, revenue forecast |
| Sales Performance | Revenue by salesperson, monthly target variance, YTD vs target by quarter |
| Product Intelligence | Profit margin by product, buying group profit stream, volume vs revenue scatter |
| Territory and Customers | Shipping days by territory, rolling 12M forecast, city map, top 20 customers |
| City Detail | Hidden drill through page — state-level city breakdown triggered from Territory page |

## Key Technical Features
- Three-tier incremental pipeline — sp_LoadLatestMonthToDEV detects latest month in SOURCE not present in PROD at month-level granularity and stages it into DEV. sp_PromoteDEVtoPROD validates inline, promotes only new months inside a transaction, and validates PROD after promotion
- View-level column standardisation — dbo.Sale unchanged. vw_Sales_Base exposes 12 columns with clean camelCase aliases and WITH SCHEMABINDING
- 25 standalone DAX measures — time intelligence written individually for Revenue and Profit allowing deliberate placement per visual
- Field parameter metric switcher — Revenue / Profit switching on Pages 1 and 3
- Hierarchy-aware RLS — USERPRINCIPALNAME() + PATH() / PATHCONTAINS() on dim_Employee
- Drill through — Territory and Customers → hidden City Detail page
- Forecast visual — MonthStart date grain enabling continuous time axis
- Conditional formatting — Revenue vs Target cards green/red based on target attainment
- Inactive relationships activated via USERELATIONSHIP() in Target Revenue measure

## Dataset
- Source: Wide World Importers — retail sales dataset
- Transactions: 228,265
- Period: May 2017 to October 2020
- Revenue: $172M
- Profit Margin: 49.8% average
- Customers: 403
- Products: 672
- Salespeople: 8
- Sales Territories: 9 US regions

## Repository Structure
```
MechmanRetail-BI/
├── README.md
├── .gitignore
├── sql/
│   └── Phase1_Implementation_Final.sql
├── docs/
│   ├── Phase1_Narrative_Final.docx
│   ├── Phase1_Guide_Final.docx
│   ├── Phase2_Narrative_Final.docx
│   ├── Phase3_Narrative_Final.docx
│   ├── Phase3_Guide_Final.docx
│   ├── Phase4_Narrative_Final.docx
│   ├── Phase4_Guide_Final.docx
│   └── MechmanRetail_User_Guide.docx
├── reports/
│   └── Mechman.pdf
└── screenshots/
    ├── page1_executive_summary.png
    ├── page2_sales_performance.png
    ├── page3_product_intelligence.png
    ├── page4_territory_customers.png
    └── city_detail_drillthrough.png
```

## Tools
SQL Server 2019 · Power BI Desktop · Power Query · DAX · Power BI Service · Novypro · GitHub

## Author
**Adiga Triumphant** — Data Analyst | Power BI Developer
[LinkedIn](linkedin.com/in/triumphant-adiga-aa297915b)
```
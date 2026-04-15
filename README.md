# Retail Sales SQL Analysis — Snowflake Star Schema

![Snowflake](https://img.shields.io/badge/Snowflake-SQL-29B5E8?style=flat&logo=snowflake&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Advanced-4479A1?style=flat)
![Schema](https://img.shields.io/badge/Model-Star%20Schema-1F4E79?style=flat)

**Skills:** Snowflake · SQL · Star Schema Design · Window Functions · CTEs · Subqueries · Aggregations · JOIN Patterns · LAG · ROW_NUMBER · RANK

> Built a retail star schema in Snowflake with 2 fact tables and 5 dimension tables, then wrote 30 business analysis queries spanning revenue trends, customer segmentation, promotion effectiveness, product performance, and revenue drop detection — demonstrating that discount depth and net revenue must always be analyzed together to avoid misleading conclusions.

---

## Overview

This project designs and populates a retail data warehouse in Snowflake using a star schema, then layers business analysis on top through structured SQL queries.

The schema models a Canadian retail business operating across Online and Retail channels, with sales transactions, web traffic events, and promotional campaigns all linked through shared dimensions. The 30 analysis queries are organized across 6 sections — progressing from basic filtering and aggregations through to advanced window functions, CTEs, and multi-level subqueries.

---

## Data Model

A **star schema** was built in `DEMO_DW.SALES` with two fact tables and five shared dimension tables.

### Fact Tables

| Table | Description | Key Metrics |
|---|---|---|
| `FACT_SALES` | 100 sales transactions | QTY, UNIT_PRICE, DISCOUNT_PCT |
| `FACT_WEB_TRAFFIC` | 100 web traffic events | SESSIONS, PAGE_VIEWS, ADD_TO_CARTS, CHECKOUTS, ORDERS |

### Dimension Tables

| Table | Description | Key Fields |
|---|---|---|
| `DIM_DATE` | 100 continuous days from 2024-01-01 | DATE_KEY, MONTH_NAME, QUARTER |
| `DIM_CUSTOMER` | 100 synthetic customers | CUSTOMER_ID, SEGMENT (Consumer/SMB/Enterprise) |
| `DIM_PRODUCT` | 100 products | CATEGORY, SUBCATEGORY, UNIT_PRICE, ACTIVE |
| `DIM_STORE` | 100 stores | REGION, CITY, CHANNEL (Online/Retail) |
| `DIM_PROMOTION` | 100 promotions | PROMO_TYPE, DISCOUNT_PCT |

### Relationships

```
FACT_SALES[DATE_KEY]     → DIM_DATE[DATE_KEY]
FACT_SALES[CUSTOMER_KEY] → DIM_CUSTOMER[CUSTOMER_KEY]
FACT_SALES[PRODUCT_KEY]  → DIM_PRODUCT[PRODUCT_KEY]
FACT_SALES[STORE_KEY]    → DIM_STORE[STORE_KEY]
FACT_SALES[PROMO_KEY]    → DIM_PROMOTION[PROMO_KEY]

FACT_WEB_TRAFFIC[DATE_KEY]    → DIM_DATE[DATE_KEY]
FACT_WEB_TRAFFIC[STORE_KEY]   → DIM_STORE[STORE_KEY]
FACT_WEB_TRAFFIC[PRODUCT_KEY] → DIM_PRODUCT[PRODUCT_KEY]
```

Two reusable views are included: `V_SALES_ENRICHED` and `V_TRAFFIC_ENRICHED`.

---

## Analysis Queries

30 queries organized across 6 sections in `analysis.sql`.

| Section | Focus | Techniques |
|---|---|---|
| 1. Basic SELECT / Filter / Sort | Customer list, active products, live promotions, regional stores | `WHERE`, `ORDER BY`, `BETWEEN`, `IN`, `LIMIT`, `SPLIT_PART` |
| 2. Aggregations & KPIs | Total revenue, category performance, promotion effectiveness | `SUM`, `AVG`, `COUNT`, `GROUP BY`, `HAVING` |
| 3. JOIN Patterns | Full transaction view, never-purchased customers, never-sold products | `INNER JOIN`, `LEFT JOIN`, `UNION`, multi-table JOIN |
| 4. Conditional Logic | Revenue bands, email filtering, above-average products and customers | `CASE WHEN`, `LIKE`, nested subqueries |
| 5. Window Functions | Customer revenue totals, product ranking within category, running totals, first orders | `SUM OVER`, `AVG OVER`, `RANK`, `ROW_NUMBER`, `LAG` |
| 6. Advanced Challenge | Top customers, biggest discount type, best store-product pair, revenue drop detection | CTEs, `LAG`, multi-level subqueries |

### Highlight Queries

```sql
-- First order per customer using ROW_NUMBER
SELECT * FROM (
    SELECT
        CUSTOMER_KEY, ORDER_ID, DATE_KEY,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_KEY ORDER BY DATE_KEY) AS RN
    FROM FACT_SALES) t
WHERE RN = 1;

-- Days where revenue dropped below previous day using LAG + CTE
WITH daily_revenue AS (
    SELECT FULL_DATE, SUM(REVENUE) AS DAILY_REVENUE
    FROM V_SALES_ENRICHED GROUP BY FULL_DATE)
SELECT FULL_DATE, DAILY_REVENUE,
    LAG(DAILY_REVENUE) OVER (ORDER BY FULL_DATE) AS PREV_DAY_REVENUE
FROM daily_revenue
WHERE DAILY_REVENUE < LAG(DAILY_REVENUE) OVER (ORDER BY FULL_DATE);

-- Customers with above-average revenue (three-level nested subquery)
SELECT FIRST_NAME, LAST_NAME
FROM DIM_CUSTOMER
WHERE CUSTOMER_KEY IN (
    SELECT CUSTOMER_KEY FROM FACT_SALES
    GROUP BY CUSTOMER_KEY
    HAVING SUM(QTY * UNIT_PRICE) > (
        SELECT AVG(TOTAL_REVENUE) FROM (
            SELECT SUM(QTY * UNIT_PRICE) AS TOTAL_REVENUE
            FROM FACT_SALES GROUP BY CUSTOMER_KEY) t));
```

---

## Key Findings

**1. Not all promotions are equally efficient — discount type drives the highest total discount given away.**
Query 2.9 identifies which promotion type generates the highest average revenue per order. Query 6.2 shows which type gives away the most in total discount dollars. These two answers are not always the same promotion type — which is exactly the point: high revenue and high discount efficiency are different things.

**2. A meaningful share of customers and products never appear in transactions.**
The LEFT JOIN queries in Section 3 identify customers who never purchased and products that were never sold. In a real dataset this would flag data quality issues or catalog bloat — inactive listings consuming shelf space without generating revenue.

**3. Revenue does not grow steadily — there are identifiable days where it drops.**
The LAG-based query in Section 6 detects day-over-day revenue declines. In a real business context this pattern would trigger investigation into promotional timing, inventory gaps, or demand seasonality.

**4. Customer revenue is concentrated — a small group drives disproportionate total.**
The top-5 customer query combined with the window function ranking shows both the absolute leaders and each customer's contribution relative to peers — two different views of the same concentration problem.

---

## What I'd Build With Real Data

With a production dataset, this model would be extended to include:

- **SCD Type 2 on DIM_CUSTOMER and DIM_PRODUCT** — to track segment and price history over time
- **Promotion lift analysis** — measuring incremental revenue above baseline during active windows
- **Cohort retention analysis** — tracking whether customers return after first purchase by segment
- **dbt transformation layer** — moving raw fact/dim loading into version-controlled, tested SQL models
- **Automated revenue drop alerting** — productionizing the LAG-based drop detection query as a scheduled job

---

## Tools Used

| Tool | Usage |
|---|---|
| Snowflake | Data warehouse, schema design, query execution |
| SQL | DDL, DML, aggregations, multi-table JOINs, window functions, CTEs, subqueries |
| Star Schema | Data modeling architecture |

---

## Key Learning

This project reinforced that **SQL fluency is not just about syntax — it is about knowing which pattern to reach for.** A `LEFT JOIN WHERE NULL` is not the same question as a subquery with `NOT IN`, even though both can find missing records. A `RANK()` window function answers a different question than `ROW_NUMBER()`, even though both produce row-level rankings. The 30 queries in this project deliberately use different patterns for similar problems — because pattern selection is the actual skill.

---

## File Structure

```
├── schema.sql      # Star schema DDL + synthetic data load (Snowflake)
├── analysis.sql    # 30 business analysis queries across 6 sections
└── README.md

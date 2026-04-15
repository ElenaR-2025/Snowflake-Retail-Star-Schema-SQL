-- ==========================================================
-- SNOWFLAKE DEMO STAR SCHEMA (Retail) — 100 rows per table
-- 2 FACTS  : FACT_SALES, FACT_WEB_TRAFFIC
-- 5 DIMs   : DIM_DATE, DIM_CUSTOMER, DIM_PRODUCT, DIM_STORE, DIM_PROMOTION
-- Safe to re-run: drops existing tables, recreates, bulk-loads data
-- ==========================================================

-- Housekeeping
CREATE DATABASE IF NOT EXISTS DEMO_DW;
CREATE SCHEMA IF NOT EXISTS DEMO_DW.SALES;
USE SCHEMA DEMO_DW.SALES;

-- Drop in dependency order (facts first)
DROP TABLE IF EXISTS FACT_WEB_TRAFFIC;
DROP TABLE IF EXISTS FACT_SALES;
DROP TABLE IF EXISTS DIM_PROMOTION;
DROP TABLE IF EXISTS DIM_STORE;
DROP TABLE IF EXISTS DIM_PRODUCT;
DROP TABLE IF EXISTS DIM_CUSTOMER;
DROP TABLE IF EXISTS DIM_DATE;

-- ======================
-- Dimensions (DDL)
-- ======================

CREATE TABLE DIM_DATE (
  DATE_KEY     NUMBER(9,0) PRIMARY KEY,   -- yyyymmdd
  FULL_DATE    DATE        NOT NULL,
  YEAR         NUMBER(4,0) NOT NULL,
  MONTH        NUMBER(2,0) NOT NULL,
  DAY          NUMBER(2,0) NOT NULL,
  MONTH_NAME   VARCHAR,
  QUARTER      NUMBER(1,0)
);

CREATE TABLE DIM_CUSTOMER (
  CUSTOMER_KEY  NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  CUSTOMER_ID   VARCHAR,
  FIRST_NAME    VARCHAR,
  LAST_NAME     VARCHAR,
  EMAIL         VARCHAR,
  SEGMENT       VARCHAR,     -- Consumer / SMB / Enterprise
  SIGNUP_DATE   DATE
);

CREATE TABLE DIM_PRODUCT (
  PRODUCT_KEY   NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  PRODUCT_ID    VARCHAR,
  PRODUCT_NAME  VARCHAR,
  CATEGORY      VARCHAR,
  SUBCATEGORY   VARCHAR,
  UNIT_PRICE    NUMBER(10,2),
  ACTIVE        BOOLEAN
);

CREATE TABLE DIM_STORE (
  STORE_KEY     NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  STORE_ID      VARCHAR,
  STORE_NAME    VARCHAR,
  REGION        VARCHAR,
  CITY          VARCHAR,
  CHANNEL       VARCHAR      -- Online / Retail
);

CREATE TABLE DIM_PROMOTION (
  PROMO_KEY     NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  PROMO_ID      VARCHAR,
  PROMO_TYPE    VARCHAR,     -- Loyalty / Discount / Bundle / BOGO / None
  DISCOUNT_PCT  NUMBER(5,2),
  START_DATE    DATE,
  END_DATE      DATE
);

-- ======================
-- Dimensions (100-row loads)
-- ======================


-- ======================
-- DIM_DATE (fixed: 100 distinct dates)
-- ======================


-- Load 100 continuous days starting 2024-01-01 (no LATERAL; single generator)
INSERT INTO DIM_DATE (DATE_KEY, FULL_DATE, YEAR, MONTH, DAY, MONTH_NAME, QUARTER)
WITH dates AS (
  SELECT DATEADD('day', SEQ4(), TO_DATE('2024-01-01')) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT
  TO_NUMBER(TO_CHAR(d,'YYYYMMDD')) AS DATE_KEY,
  d                                 AS FULL_DATE,
  YEAR(d)                           AS YEAR,
  MONTH(d)                          AS MONTH,
  DAY(d)                            AS DAY,
  RTRIM(TO_CHAR(d,'Month'))         AS MONTH_NAME,
  QUARTER(d)                        AS QUARTER
FROM dates;

-- (Optional quick check)
-- SELECT COUNT(*) AS rows, COUNT(DISTINCT FULL_DATE) AS distinct_dates FROM DIM_DATE;
-- SELECT * FROM DIM_DATE ORDER BY FULL_DATE LIMIT 25;









-- DIM_CUSTOMER: 100 synthetic customers
INSERT INTO DIM_CUSTOMER (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, SEGMENT, SIGNUP_DATE)
WITH gen AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT
  'C' || LPAD(TO_VARCHAR(1000 + n), 4, '0')  AS CUSTOMER_ID,
  'First_' || n                              AS FIRST_NAME,
  'Last_'  || n                              AS LAST_NAME,
  'user' || n || '@example.com'              AS EMAIL,
  CASE MOD(n,3)
    WHEN 0 THEN 'Consumer'
    WHEN 1 THEN 'SMB'
    ELSE 'Enterprise'
  END                                        AS SEGMENT,
  DATEADD('day', -UNIFORM(1,180,RANDOM()), '2024-01-01') AS SIGNUP_DATE
FROM gen;

-- DIM_PRODUCT: 100 products
INSERT INTO DIM_PRODUCT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUBCATEGORY, UNIT_PRICE, ACTIVE)
WITH gen AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT
  'P' || LPAD(TO_VARCHAR(200 + n), 3, '0')                      AS PRODUCT_ID,
  CASE MOD(n,5)
    WHEN 0 THEN 'LED Strip Light '   || n
    WHEN 1 THEN 'Picture Frame Set ' || n
    WHEN 2 THEN 'Toaster Oven '      || n
    WHEN 3 THEN 'Blender '           || n
    ELSE      'Smart Lamp '          || n
  END                                                           AS PRODUCT_NAME,
  CASE MOD(n,3)
    WHEN 0 THEN 'Accessories'
    WHEN 1 THEN 'Kitchen'
    ELSE 'Home'
  END                                                           AS CATEGORY,
  CASE MOD(n,5)
    WHEN 0 THEN 'Lighting'
    WHEN 1 THEN 'Decor'
    WHEN 2 THEN 'Appliances'
    WHEN 3 THEN 'Appliances'
    ELSE 'Lighting'
  END                                                           AS SUBCATEGORY,
  ROUND(UNIFORM(15, 250, RANDOM()), 2)                          AS UNIT_PRICE,
  CASE WHEN MOD(n,10) < 8 THEN TRUE ELSE FALSE END             AS ACTIVE
FROM gen;

-- DIM_STORE: 100 stores
INSERT INTO DIM_STORE (STORE_ID, STORE_NAME, REGION, CITY, CHANNEL)
WITH gen AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT
  'S' || LPAD(TO_VARCHAR(1 + n), 3, '0')                           AS STORE_ID,
  CASE WHEN MOD(n,5)=0 THEN 'Online CA ' || n
       ELSE 'Store ' || n END                                       AS STORE_NAME,
  CASE MOD(n,5)
    WHEN 0 THEN 'Canada'
    WHEN 1 THEN 'Prairies'
    WHEN 2 THEN 'Ontario'
    WHEN 3 THEN 'BC'
    ELSE 'Alberta'
  END                                                               AS REGION,
  CASE MOD(n,6)
    WHEN 0 THEN 'Calgary'
    WHEN 1 THEN 'Edmonton'
    WHEN 2 THEN 'Toronto'
    WHEN 3 THEN 'Vancouver'
    WHEN 4 THEN 'Winnipeg'
    ELSE 'Ottawa'
  END                                                               AS CITY,
  CASE WHEN MOD(n,5)=0 THEN 'Online' ELSE 'Retail' END             AS CHANNEL
FROM gen;

-- DIM_PROMOTION: 100 promos
INSERT INTO DIM_PROMOTION (PROMO_ID, PROMO_TYPE, DISCOUNT_PCT, START_DATE, END_DATE)
WITH gen AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT
  'PR' || LPAD(TO_VARCHAR(10 + n), 3, '0')                                     AS PROMO_ID,
  CASE MOD(n,5)
    WHEN 0 THEN 'Loyalty'
    WHEN 1 THEN 'Discount'
    WHEN 2 THEN 'Bundle'
    WHEN 3 THEN 'BOGO'
    ELSE 'None'
  END                                                                           AS PROMO_TYPE,
  CASE MOD(n,5)
    WHEN 0 THEN 0.03
    WHEN 1 THEN 0.10
    WHEN 2 THEN 0.15
    WHEN 3 THEN 0.50
    ELSE 0.00
  END                                                                           AS DISCOUNT_PCT,
  DATEADD('day', MOD(n, 60), '2024-01-01')                                     AS START_DATE,
  DATEADD('day', MOD(n, 60) + CASE MOD(n,5) WHEN 3 THEN 2 ELSE 10 END, '2024-01-01') AS END_DATE
FROM gen;

-- ======================
-- Facts (DDL)
-- ======================

CREATE TABLE FACT_SALES (
  SALES_ID      NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  DATE_KEY      NUMBER(9,0)   NOT NULL,
  CUSTOMER_KEY  NUMBER(38,0),
  PRODUCT_KEY   NUMBER(38,0)  NOT NULL,
  STORE_KEY     NUMBER(38,0)  NOT NULL,
  PROMO_KEY     NUMBER(38,0),

  ORDER_ID      VARCHAR,
  QTY           NUMBER(10,2)  NOT NULL,
  UNIT_PRICE    NUMBER(10,2)  NOT NULL,
  DISCOUNT_PCT  NUMBER(5,2)   DEFAULT 0
);

CREATE TABLE FACT_WEB_TRAFFIC (
  TRAFFIC_ID    NUMBER(38,0) IDENTITY START 1 INCREMENT 1 PRIMARY KEY,
  DATE_KEY      NUMBER(9,0)   NOT NULL,
  STORE_KEY     NUMBER(38,0)  NOT NULL,
  PRODUCT_KEY   NUMBER(38,0),

  SESSIONS      NUMBER(12,0),
  PAGE_VIEWS    NUMBER(12,0),
  ADD_TO_CARTS  NUMBER(12,0),
  CHECKOUTS     NUMBER(12,0),
  ORDERS        NUMBER(12,0)
);

-- ======================
-- Facts (100-row loads)
-- ======================

-- FACT_SALES — 100 rows (no g.n; using inline subquery alias 'gen')
INSERT INTO FACT_SALES
  (DATE_KEY, CUSTOMER_KEY, PRODUCT_KEY, STORE_KEY, PROMO_KEY, ORDER_ID, QTY, UNIT_PRICE, DISCOUNT_PCT)
WITH
  d   AS (SELECT DATE_KEY, ROW_NUMBER() OVER (ORDER BY DATE_KEY) AS rn FROM DIM_DATE),
  c   AS (SELECT CUSTOMER_KEY, ROW_NUMBER() OVER (ORDER BY CUSTOMER_KEY) AS rn FROM DIM_CUSTOMER),
  p   AS (SELECT PRODUCT_KEY, UNIT_PRICE, ROW_NUMBER() OVER (ORDER BY PRODUCT_KEY) AS rn FROM DIM_PRODUCT),
  s   AS (SELECT STORE_KEY, CHANNEL, ROW_NUMBER() OVER (ORDER BY STORE_KEY) AS rn FROM DIM_STORE),
  m   AS (SELECT PROMO_KEY, DISCOUNT_PCT, ROW_NUMBER() OVER (ORDER BY PROMO_KEY) AS rn FROM DIM_PROMOTION),
  cnt AS (
    SELECT (SELECT MAX(rn) FROM d) AS cd,
           (SELECT MAX(rn) FROM c) AS cc,
           (SELECT MAX(rn) FROM p) AS cp,
           (SELECT MAX(rn) FROM s) AS cs,
           (SELECT MAX(rn) FROM m) AS cm
  )
SELECT
  d.DATE_KEY,
  c.CUSTOMER_KEY,
  p.PRODUCT_KEY,
  s.STORE_KEY,
  m.PROMO_KEY,
  'O-' || LPAD(TO_VARCHAR(1000 + gen.n), 5, '0') AS ORDER_ID,
  CAST(UNIFORM(1, 5, RANDOM()) AS NUMBER(10,2))  AS QTY,
  p.UNIT_PRICE                                    AS UNIT_PRICE,
  m.DISCOUNT_PCT                                  AS DISCOUNT_PCT
FROM (SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))) gen
CROSS JOIN cnt
JOIN d ON d.rn = MOD(gen.n, cnt.cd) + 1
JOIN c ON c.rn = MOD(gen.n, cnt.cc) + 1
JOIN p ON p.rn = MOD(gen.n, cnt.cp) + 1
JOIN s ON s.rn = MOD(gen.n, cnt.cs) + 1
JOIN m ON m.rn = MOD(gen.n, cnt.cm) + 1;

-- FACT_WEB_TRAFFIC — 100 rows (same pattern)
INSERT INTO FACT_WEB_TRAFFIC
  (DATE_KEY, STORE_KEY, PRODUCT_KEY, SESSIONS, PAGE_VIEWS, ADD_TO_CARTS, CHECKOUTS, ORDERS)
WITH
  d   AS (SELECT DATE_KEY, ROW_NUMBER() OVER (ORDER BY DATE_KEY) AS rn FROM DIM_DATE),
  p   AS (SELECT PRODUCT_KEY, ROW_NUMBER() OVER (ORDER BY PRODUCT_KEY) AS rn FROM DIM_PRODUCT),
  s   AS (SELECT STORE_KEY, ROW_NUMBER() OVER (ORDER BY STORE_KEY) AS rn FROM DIM_STORE),
  cnt AS (
    SELECT (SELECT MAX(rn) FROM d) AS cd,
           (SELECT MAX(rn) FROM p) AS cp,
           (SELECT MAX(rn) FROM s) AS cs
  )
SELECT
  d.DATE_KEY,
  s.STORE_KEY,
  p.PRODUCT_KEY,
  UNIFORM(200, 5000, RANDOM())  AS SESSIONS,
  UNIFORM(500, 12000, RANDOM()) AS PAGE_VIEWS,
  UNIFORM(10, 400, RANDOM())    AS ADD_TO_CARTS,
  UNIFORM(5, 200, RANDOM())     AS CHECKOUTS,
  UNIFORM(1, 150, RANDOM())     AS ORDERS
FROM (SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 100))) gen
CROSS JOIN cnt
JOIN d ON d.rn = MOD(gen.n, cnt.cd) + 1
JOIN p ON p.rn = MOD(gen.n, cnt.cp) + 1
JOIN s ON s.rn = MOD(gen.n, cnt.cs) + 1;

-- ======================
-- Convenience views
-- ======================

CREATE OR REPLACE VIEW V_SALES_ENRICHED AS
SELECT
  fs.SALES_ID,
  dd.FULL_DATE,
  st.STORE_NAME,
  st.CHANNEL,
  pr.PRODUCT_NAME,
  pr.CATEGORY,
  fs.QTY,
  fs.UNIT_PRICE,
  fs.DISCOUNT_PCT,
  ROUND(fs.QTY * fs.UNIT_PRICE * (1 - fs.DISCOUNT_PCT), 2) AS REVENUE
FROM FACT_SALES fs
JOIN DIM_DATE     dd ON fs.DATE_KEY   = dd.DATE_KEY
JOIN DIM_STORE    st ON fs.STORE_KEY  = st.STORE_KEY
JOIN DIM_PRODUCT  pr ON fs.PRODUCT_KEY= pr.PRODUCT_KEY;

CREATE OR REPLACE VIEW V_TRAFFIC_ENRICHED AS
SELECT
  wt.TRAFFIC_ID,
  dd.FULL_DATE,
  st.STORE_NAME,
  pr.PRODUCT_NAME,
  wt.SESSIONS,
  wt.PAGE_VIEWS,
  wt.ADD_TO_CARTS,
  wt.CHECKOUTS,
  wt.ORDERS
FROM FACT_WEB_TRAFFIC wt
JOIN DIM_DATE dd  ON wt.DATE_KEY  = dd.DATE_KEY
JOIN DIM_STORE st ON wt.STORE_KEY = st.STORE_KEY
LEFT JOIN DIM_PRODUCT pr ON wt.PRODUCT_KEY = pr.PRODUCT_KEY;

-- ======================
-- Quick sanity checks
-- ======================

-- Row counts (should all be 100)
SELECT 'DIM_DATE' tbl, COUNT(*) cnt FROM DIM_DATE UNION ALL
SELECT 'DIM_CUSTOMER', COUNT(*) FROM DIM_CUSTOMER UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT UNION ALL
SELECT 'DIM_STORE', COUNT(*) FROM DIM_STORE UNION ALL
SELECT 'DIM_PROMOTION', COUNT(*) FROM DIM_PROMOTION UNION ALL
SELECT 'FACT_SALES', COUNT(*) FROM FACT_SALES UNION ALL
SELECT 'FACT_WEB_TRAFFIC', COUNT(*) FROM FACT_WEB_TRAFFIC;

-- Sample sales rollup
SELECT FULL_DATE, CHANNEL, CATEGORY, SUM(REVENUE) AS SALES
FROM V_SALES_ENRICHED
GROUP BY 1,2,3
ORDER BY 1,2,3
LIMIT 20;

-- Sample traffic snapshot
SELECT FULL_DATE, STORE_NAME, PRODUCT_NAME, SESSIONS, PAGE_VIEWS, ADD_TO_CARTS, CHECKOUTS, ORDERS
FROM V_TRAFFIC_ENRICHED
ORDER BY FULL_DATE, STORE_NAME, PRODUCT_NAME
LIMIT 20;
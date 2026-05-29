# Dashboard Metrics Specification

Tai lieu nay la phan ban giao cua nhom data/metrics cho BE va FE.

Muc tieu cua nguoi lam metrics:

- Dinh nghia dung y nghia tung chi so.
- Chi ro Gold table nao la source of truth.
- Cung cap SQL chuan de BE expose API.
- Cung cap response mau de FE mock chart.
- Cung cap validation query de kiem tra so lieu truoc khi demo.

Nguoi lam metrics khong can implement FE/BE. BE chi can boc cac SQL nay thanh
API, FE chi can render theo response shape.

## Data Source

Dashboard doc tu Gold layer qua Trino:

- `iceberg.gold.daily_event_summary`
- `iceberg.gold.daily_brand_summary`
- `iceberg.gold.daily_category_summary`
- `iceberg.gold.daily_product_summary`
- `iceberg.gold.fact_events`
- `iceberg.gold.fact_sales`

Mac dinh filter ngay dung ISO date: `YYYY-MM-DD`.

Quy uoc chung:

- `as_of_date`: ngay chot so lieu dashboard. Neu khong truyen, dung
  `max(event_date)` trong `daily_event_summary`.
- `start_date`, `end_date`: filter inclusive, nghia la lay ca 2 ngay dau/cuoi.
- `MTD`: month-to-date, tinh tu ngay dau thang cua `as_of_date` den
  `as_of_date`.
- `revenue`: doanh thu gross, lay tu purchase events, khong tru hoan/discount.
- `conversion_rate`: `purchases / views`.
- `cart_to_purchase_rate`: `purchases / carts`.
- Neu denominator bang 0 thi rate = `0.0`.
- Unknown/null dimension nen hien thi la `unknown` o tang presentation/API.

## Metric Definitions

| Metric | Definition | Source Table | Notes |
| --- | --- | --- | --- |
| Today revenue | Tong doanh thu cua `as_of_date` | `daily_event_summary.total_revenue` | Tren UI co the doi label thanh "Selected day revenue" neu co date picker. |
| Today events | Tong events cua `as_of_date` | `daily_event_summary.total_events` | Gom view, cart, remove_from_cart, purchase. |
| Today purchases | Tong purchase events cua `as_of_date` | `daily_event_summary.total_purchases` | Nen bang count rows trong `fact_sales` theo ngay. |
| Today conversion rate | `total_purchases / total_views` cua `as_of_date` | `daily_event_summary.conversion_rate` | Rate dang decimal, vi du `0.035` = 3.5%. |
| MTD events | Tong events tu dau thang den `as_of_date` | `daily_event_summary.total_events` | Dung month cua `as_of_date`. |
| MTD revenue | Tong revenue tu dau thang den `as_of_date` | `daily_event_summary.total_revenue` | Dung cho KPI phu hoac tooltip. |
| Top brand MTD | Brand co MTD revenue cao nhat | `daily_brand_summary.revenue` | Tie-break: purchases desc, brand asc. |
| Revenue time series | Doanh thu theo ngay | `daily_event_summary` | Dung cho line/bar chart. |
| Top brands | Brand ranking theo revenue | `daily_brand_summary` | Default top 10. |
| Category conversion | Conversion theo category L1/L2/L3 | `daily_category_summary` | Default top 10 by revenue. |
| Product leaderboard | Product ranking theo revenue | `daily_product_summary` | Default top 10. |

## Canonical SQL

BE nen dung cac SQL nay lam source chinh. Tham so trong SQL duoc viet theo dang
`:param` de BE thay bang query parameter an toan.

### KPI Overview

Input:

- `:as_of_date` optional, ISO date.

Output:

```json
{
  "event_date": "2020-01-31",
  "today_revenue": 123456.78,
  "today_events": 12345,
  "today_purchases": 456,
  "today_conversion_rate": 0.0369,
  "mtd_events": 987654,
  "mtd_revenue": 8765432.10,
  "top_brand_mtd": "samsung",
  "top_brand_mtd_revenue": 234567.89
}
```

SQL:

```sql
WITH selected_day AS (
    SELECT COALESCE(
        CAST(:as_of_date AS DATE),
        (SELECT max(event_date) FROM iceberg.gold.daily_event_summary)
    ) AS event_date
),
current_day AS (
    SELECT
        event_date,
        total_revenue,
        total_events,
        total_purchases,
        conversion_rate
    FROM iceberg.gold.daily_event_summary
    WHERE event_date = (SELECT event_date FROM selected_day)
),
month_to_date AS (
    SELECT
        sum(total_events) AS mtd_events,
        sum(total_revenue) AS mtd_revenue
    FROM iceberg.gold.daily_event_summary
    WHERE event_date >= date_trunc('month', (SELECT event_date FROM selected_day))
      AND event_date <= (SELECT event_date FROM selected_day)
),
top_brand AS (
    SELECT
        brand,
        sum(revenue) AS revenue,
        sum(purchase_count) AS purchases
    FROM iceberg.gold.daily_brand_summary
    WHERE event_date >= date_trunc('month', (SELECT event_date FROM selected_day))
      AND event_date <= (SELECT event_date FROM selected_day)
    GROUP BY brand
    ORDER BY revenue DESC, purchases DESC, brand ASC
    LIMIT 1
)
SELECT
    current_day.event_date,
    current_day.total_revenue AS today_revenue,
    current_day.total_events AS today_events,
    current_day.total_purchases AS today_purchases,
    current_day.conversion_rate AS today_conversion_rate,
    month_to_date.mtd_events,
    month_to_date.mtd_revenue,
    top_brand.brand AS top_brand_mtd,
    top_brand.revenue AS top_brand_mtd_revenue
FROM current_day
CROSS JOIN month_to_date
LEFT JOIN top_brand ON true;
```

### Revenue Time Series

Input:

- `:start_date` optional.
- `:end_date` optional.

Output:

```json
[
  {
    "event_date": "2020-01-01",
    "total_revenue": 12345.67,
    "total_events": 1200,
    "total_purchases": 45,
    "conversion_rate": 0.0375,
    "cart_to_purchase_rate": 0.22
  }
]
```

SQL:

```sql
SELECT
    event_date,
    total_revenue,
    total_events,
    total_purchases,
    conversion_rate,
    cart_to_purchase_rate
FROM iceberg.gold.daily_event_summary
WHERE (:start_date IS NULL OR event_date >= CAST(:start_date AS DATE))
  AND (:end_date IS NULL OR event_date <= CAST(:end_date AS DATE))
ORDER BY event_date ASC;
```

### Top Brands

Input:

- `:start_date` optional.
- `:end_date` optional.
- `:limit` default `10`, max `100`.

Output:

```json
[
  {
    "brand": "samsung",
    "revenue": 123456.78,
    "views": 10000,
    "carts": 1200,
    "purchases": 350,
    "conversion_rate": 0.035
  }
]
```

SQL:

```sql
SELECT
    COALESCE(brand, 'unknown') AS brand,
    sum(revenue) AS revenue,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate
FROM iceberg.gold.daily_brand_summary
WHERE (:start_date IS NULL OR event_date >= CAST(:start_date AS DATE))
  AND (:end_date IS NULL OR event_date <= CAST(:end_date AS DATE))
GROUP BY COALESCE(brand, 'unknown')
ORDER BY revenue DESC, purchases DESC, brand ASC
LIMIT :limit;
```

### Category Conversion

Input:

- `:start_date` optional.
- `:end_date` optional.
- `:limit` default `10`, max `100`.

Output:

```json
[
  {
    "category_l1": "electronics",
    "category_l2": "smartphone",
    "category_l3": "android",
    "total_events": 20000,
    "views": 15000,
    "carts": 1800,
    "purchases": 500,
    "revenue": 234567.89,
    "conversion_rate": 0.0333,
    "cart_to_purchase_rate": 0.2778
  }
]
```

SQL:

```sql
SELECT
    COALESCE(category_l1, 'unknown') AS category_l1,
    COALESCE(category_l2, 'unknown') AS category_l2,
    COALESCE(category_l3, 'unknown') AS category_l3,
    sum(total_events) AS total_events,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    sum(revenue) AS revenue,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate,
    CASE
        WHEN sum(cart_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(cart_count) AS DOUBLE)
    END AS cart_to_purchase_rate
FROM iceberg.gold.daily_category_summary
WHERE (:start_date IS NULL OR event_date >= CAST(:start_date AS DATE))
  AND (:end_date IS NULL OR event_date <= CAST(:end_date AS DATE))
GROUP BY
    COALESCE(category_l1, 'unknown'),
    COALESCE(category_l2, 'unknown'),
    COALESCE(category_l3, 'unknown')
ORDER BY revenue DESC, purchases DESC, category_l1 ASC, category_l2 ASC, category_l3 ASC
LIMIT :limit;
```

### Product Leaderboard

Input:

- `:start_date` optional.
- `:end_date` optional.
- `:limit` default `10`, max `100`.

Output:

```json
[
  {
    "product_id": 123,
    "brand": "samsung",
    "category_l1": "electronics",
    "category_l2": "smartphone",
    "category_l3": "android",
    "revenue": 12345.67,
    "views": 1200,
    "carts": 130,
    "purchases": 45,
    "conversion_rate": 0.0375
  }
]
```

SQL:

```sql
SELECT
    product_id,
    COALESCE(brand, 'unknown') AS brand,
    COALESCE(category_l1, 'unknown') AS category_l1,
    COALESCE(category_l2, 'unknown') AS category_l2,
    COALESCE(category_l3, 'unknown') AS category_l3,
    sum(revenue) AS revenue,
    sum(view_count) AS views,
    sum(cart_count) AS carts,
    sum(purchase_count) AS purchases,
    CASE
        WHEN sum(view_count) = 0 THEN 0.0
        ELSE CAST(sum(purchase_count) AS DOUBLE) / CAST(sum(view_count) AS DOUBLE)
    END AS conversion_rate
FROM iceberg.gold.daily_product_summary
WHERE (:start_date IS NULL OR event_date >= CAST(:start_date AS DATE))
  AND (:end_date IS NULL OR event_date <= CAST(:end_date AS DATE))
GROUP BY
    product_id,
    COALESCE(brand, 'unknown'),
    COALESCE(category_l1, 'unknown'),
    COALESCE(category_l2, 'unknown'),
    COALESCE(category_l3, 'unknown')
ORDER BY revenue DESC, purchases DESC, product_id ASC
LIMIT :limit;
```

## Validation Queries

Chay cac query nay truoc khi ban giao cho BE/FE hoac truoc demo.

### Table Availability

```sql
SHOW TABLES FROM iceberg.gold;
```

Expected tables:

- `daily_event_summary`
- `daily_brand_summary`
- `daily_category_summary`
- `daily_product_summary`
- `fact_events`
- `fact_sales`

### Date Coverage

```sql
SELECT
    min(event_date) AS min_event_date,
    max(event_date) AS max_event_date,
    count(*) AS day_count
FROM iceberg.gold.daily_event_summary;
```

Expected:

- `min_event_date` and `max_event_date` are not null.
- `day_count > 0`.

### Summary Row Counts

```sql
SELECT 'daily_event_summary' AS table_name, count(*) AS row_count
FROM iceberg.gold.daily_event_summary
UNION ALL
SELECT 'daily_brand_summary', count(*)
FROM iceberg.gold.daily_brand_summary
UNION ALL
SELECT 'daily_category_summary', count(*)
FROM iceberg.gold.daily_category_summary
UNION ALL
SELECT 'daily_product_summary', count(*)
FROM iceberg.gold.daily_product_summary;
```

Expected:

- All row counts are greater than 0 after Gold pipeline has run.

### Revenue Consistency

`daily_event_summary.total_revenue` should match `fact_sales.gross_amount` by
date.

```sql
WITH summary_revenue AS (
    SELECT event_date, sum(total_revenue) AS summary_revenue
    FROM iceberg.gold.daily_event_summary
    GROUP BY event_date
),
fact_revenue AS (
    SELECT sale_date AS event_date, sum(gross_amount) AS fact_revenue
    FROM iceberg.gold.fact_sales
    GROUP BY sale_date
)
SELECT
    COALESCE(summary_revenue.event_date, fact_revenue.event_date) AS event_date,
    summary_revenue.summary_revenue,
    fact_revenue.fact_revenue,
    COALESCE(summary_revenue.summary_revenue, 0) - COALESCE(fact_revenue.fact_revenue, 0)
        AS revenue_diff
FROM summary_revenue
FULL OUTER JOIN fact_revenue
    ON summary_revenue.event_date = fact_revenue.event_date
WHERE abs(
    CAST(COALESCE(summary_revenue.summary_revenue, 0) AS DOUBLE)
    - CAST(COALESCE(fact_revenue.fact_revenue, 0) AS DOUBLE)
) > 0.01
ORDER BY event_date;
```

Expected:

- Query returns 0 rows.

### Purchase Count Consistency

`daily_event_summary.total_purchases` should match count of purchase rows in
`fact_sales`.

```sql
WITH summary_purchases AS (
    SELECT event_date, sum(total_purchases) AS summary_purchases
    FROM iceberg.gold.daily_event_summary
    GROUP BY event_date
),
fact_purchases AS (
    SELECT sale_date AS event_date, count(*) AS fact_purchases
    FROM iceberg.gold.fact_sales
    GROUP BY sale_date
)
SELECT
    COALESCE(summary_purchases.event_date, fact_purchases.event_date) AS event_date,
    summary_purchases.summary_purchases,
    fact_purchases.fact_purchases,
    COALESCE(summary_purchases.summary_purchases, 0)
      - COALESCE(fact_purchases.fact_purchases, 0) AS purchase_diff
FROM summary_purchases
FULL OUTER JOIN fact_purchases
    ON summary_purchases.event_date = fact_purchases.event_date
WHERE COALESCE(summary_purchases.summary_purchases, 0)
    <> COALESCE(fact_purchases.fact_purchases, 0)
ORDER BY event_date;
```

Expected:

- Query returns 0 rows.

### Invalid Rate Values

```sql
SELECT *
FROM iceberg.gold.daily_event_summary
WHERE conversion_rate < 0
   OR conversion_rate > 1
   OR cart_to_purchase_rate < 0;
```

Expected:

- Query returns 0 rows.
- `cart_to_purchase_rate` can be greater than 1 only if data has more purchases
  than cart events; normally it should also be `<= 1`, but do not fail demo on
  this until event semantics are confirmed.

## Edge Cases

BE/FE should handle these cases:

- Empty result: show `-` or empty chart, not error UI.
- No brand/category: show `unknown`.
- Revenue null: treat as `0` only in presentation layer.
- Rate null: show `0%` only if denominator is 0; otherwise investigate data.
- Date outside data range: return empty response with metadata explaining the
  requested range.
- Limit greater than 100: clamp to 100.

## Suggested API Shape

BE can expose these endpoints. This is only a suggestion for BE, not part of
the metrics implementation.

- `GET /api/metrics/overview?as_of_date=2020-01-31`
- `GET /api/metrics/revenue?start_date=2020-01-01&end_date=2020-01-31`
- `GET /api/metrics/brands?start_date=2020-01-01&end_date=2020-01-31&limit=10`
- `GET /api/metrics/categories?start_date=2020-01-01&end_date=2020-01-31&limit=10`
- `GET /api/metrics/products?start_date=2020-01-01&end_date=2020-01-31&limit=10`

Common response envelope:

```json
{
  "data": {},
  "meta": {
    "start_date": "2020-01-01",
    "end_date": "2020-01-31",
    "source": "trino.iceberg.gold"
  }
}
```

## Handoff Checklist

Before handing off to BE/FE:

- Confirm Gold pipeline has produced all required tables.
- Run validation queries and save result screenshots/logs if needed.
- Confirm `conversion_rate = purchases / views` with the team.
- Confirm top brand ranking uses `revenue DESC`.
- Confirm date range for demo, for example `2020-01-01` to `2020-01-31`.
- Provide 1 sample response per metric endpoint.
- Tell FE which fields are money, count, percent, and date.

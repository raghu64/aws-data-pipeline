CREATE SCHEMA reporting;

CREATE TABLE reporting.customer_segment_value (
    customer_segment character varying(12) ENCODE zstd distkey,
    customer_count bigint ENCODE zstd,
    avg_spent double precision ENCODE zstd,
    avg_transactions bigint ENCODE zstd,
    spst_date date default CURRENT_DATE
)
DISTSTYLE KEY;

CREATE TABLE reporting.customer_activity_analysis (
    total_customers bigint ENCODE zstd,
    active_90_days bigint ENCODE zstd,
    inactive_90_days bigint ENCODE zstd,
    avg_customer_lifespan bigint ENCODE zstd,
    spst_date date default CURRENT_DATE
)
DISTSTYLE EVEN;

CREATE TABLE reporting.category_performance_analysis  (
    category character varying(100) ENCODE zstd distkey,
    category_revenue double precision ENCODE zstd,
    category_units bigint ENCODE zstd,
    total_customers bigint ENCODE zstd,
    category_avg_price double precision ENCODE zstd,
    revenue_per_customer double precision ENCODE zstd,
    spst_date date default CURRENT_DATE
)
DISTSTYLE KEY;

CREATE TABLE reporting.top_performing_subcategories  (
    category character varying(100) ENCODE zstd distkey,
    subcategory character varying(100) ENCODE zstd,
    total_revenue double precision ENCODE zstd,
    total_units_sold bigint ENCODE zstd,
    category_revenue_share double precision ENCODE zstd,
    spst_date date default CURRENT_DATE
)
DISTSTYLE KEY;

CREATE TABLE reporting.customer_purchase_frequency  (
    frequency_segment character varying(29) ENCODE zstd distkey,
    customer_count bigint ENCODE zstd,
    avg_total_spent double precision ENCODE zstd,
    avg_transaction_value double precision ENCODE zstd,
    spst_date date default CURRENT_DATE
)
DISTSTYLE KEY;

# Eraser.io syntax

```
dim_time [icon: clock, color: purple] {
  time_id string pk
  event_date date
  event_year int
  event_month int
  event_day int
  event_hour int

  day_of_week int
  day_name string
  month_name string
  quarter int
  is_weekend boolean
}

dim_product [icon: package, color: green] {
  product_id bigint pk
  category_id bigint

  category_code string
  category_l1 string
  category_l2 string
  category_l3 string

  brand string

  first_seen_at timestamp
  last_seen_at timestamp

  avg_observed_price decimal
  min_observed_price decimal
  max_observed_price decimal

  record_count bigint
  updated_at timestamp
}

dim_user [icon: user, color: blue] {
  user_id bigint pk

  first_seen_at timestamp
  last_seen_at timestamp

  total_sessions bigint
  total_events bigint
  total_views bigint
  total_cart_adds bigint
  total_remove_from_carts bigint
  total_purchases bigint
  total_revenue decimal

  updated_at timestamp
}

dim_session [icon: users, color: blue] {
  session_id string pk
  user_id bigint fk

  session_start_at timestamp
  session_end_at timestamp
  session_duration_sec bigint

  event_count bigint
  view_count bigint
  cart_count bigint
  purchase_count bigint

  session_revenue decimal
  has_purchase boolean

  updated_at timestamp
}

fact_events [icon: activity, color: orange] {
  event_id string pk
  event_fingerprint string unique
  source_event_id string

  time_id string fk
  event_ts timestamp
  event_date date
  event_type string

  product_id bigint fk
  user_id bigint fk
  session_id string fk

  price decimal

  is_view boolean
  is_cart boolean
  is_remove_from_cart boolean
  is_purchase boolean

  kafka_partition int
  kafka_offset bigint
  kafka_ts timestamp

  silver_processed_at timestamp
  gold_processed_at timestamp
}

fact_sales [icon: shopping-cart, color: red] {
  sale_id string pk
  event_fingerprint string unique
  source_event_id string

  time_id string fk
  sale_ts timestamp
  sale_date date

  product_id bigint fk
  user_id bigint fk
  session_id string fk

  unit_price decimal
  quantity int
  gross_amount decimal

  gold_processed_at timestamp
}

daily_event_summary [icon: bar-chart-2, color: yellow] {
  event_date date pk

  total_events bigint
  total_views bigint
  total_carts bigint
  total_remove_from_carts bigint
  total_purchases bigint

  unique_users bigint
  unique_sessions bigint
  unique_products bigint
  unique_events bigint

  total_revenue decimal
  avg_event_price decimal

  conversion_rate double
  cart_to_purchase_rate double

  gold_processed_at timestamp
}

daily_product_summary [icon: trending-up, color: yellow] {
  summary_id string pk

  event_date date
  product_id bigint fk

  brand string
  category_l1 string
  category_l2 string
  category_l3 string

  view_count bigint
  cart_count bigint
  purchase_count bigint
  remove_from_cart_count bigint

  unique_events bigint
  unique_users bigint
  unique_sessions bigint

  revenue decimal

  avg_price decimal
  min_price decimal
  max_price decimal

  conversion_rate double
  cart_to_purchase_rate double

  gold_processed_at timestamp
}

daily_category_summary [icon: layers, color: yellow] {
  summary_id string pk

  event_date date

  category_l1 string
  category_l2 string
  category_l3 string

  total_events bigint
  view_count bigint
  cart_count bigint
  purchase_count bigint
  remove_from_cart_count bigint

  unique_events bigint
  unique_users bigint
  unique_products bigint

  revenue decimal
  conversion_rate double
  cart_to_purchase_rate double

  gold_processed_at timestamp
}

daily_brand_summary [icon: tag, color: yellow] {
  summary_id string pk

  event_date date
  brand string

  view_count bigint
  cart_count bigint
  purchase_count bigint
  remove_from_cart_count bigint

  unique_events bigint
  unique_users bigint
  unique_products bigint

  revenue decimal
  conversion_rate double
  cart_to_purchase_rate double

  gold_processed_at timestamp
}

metadata_table_catalog [icon: database, color: gray] {
  table_name string pk
  layer string
  table_type string

  business_name string
  description string
  grain string
  primary_key string
  unique_key string

  storage_format string
  query_engine string

  is_agent_visible boolean
  recommended_for_agent boolean

  refresh_frequency string
  owner string

  created_at timestamp
  updated_at timestamp
}

metadata_column_catalog [icon: columns, color: gray] {
  column_id string pk

  table_name string fk
  column_name string

  data_type string
  business_name string
  description string

  source_table string
  source_column string
  transformation_logic string

  is_nullable boolean
  is_dimension boolean
  is_metric boolean
  is_time_column boolean
  is_join_key boolean
  is_unique_key boolean

  example_values string
  allowed_values string
  agent_synonyms string
}

metadata_metric_catalog [icon: function-square, color: gray] {
  metric_name string pk

  business_name string
  description string

  formula_sql string
  base_table string
  default_time_column string

  aggregation_type string
  unit string
  example_question string
}

metadata_join_catalog [icon: git-merge, color: gray] {
  join_id string pk

  left_table string
  left_key string

  right_table string
  right_key string

  relationship_type string
  description string
}

fact_events.time_id > dim_time.time_id
fact_events.product_id > dim_product.product_id
fact_events.user_id > dim_user.user_id
fact_events.session_id > dim_session.session_id

fact_sales.time_id > dim_time.time_id
fact_sales.product_id > dim_product.product_id
fact_sales.user_id > dim_user.user_id
fact_sales.session_id > dim_session.session_id
fact_sales.event_fingerprint > fact_events.event_fingerprint

dim_session.user_id > dim_user.user_id

daily_product_summary.product_id > dim_product.product_id

metadata_column_catalog.table_name > metadata_table_catalog.table_name
```
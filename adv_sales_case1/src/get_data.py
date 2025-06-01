from google.cloud import bigquery
from google.cloud import bigquery_storage
import pandas as pd

# Initialize BigQuery and BigQuery Storage clients
bq_client = bigquery.Client()
bq_storage_client = bigquery_storage.BigQueryReadClient()

# Function to generate date suffixes for each day in a month
def get_date_suffixes(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date).strftime('%Y%m%d').tolist()

# Define your desired months
months = [
    ('2020-11-01', '2020-11-30'),
    ('2020-12-01', '2020-12-31'),
    ('2021-01-01', '2021-01-31')
]

# Container for all DataFrames
all_dfs = []

# Query each month separately
for start_date, end_date in months:
    date_suffixes = get_date_suffixes(start_date, end_date)
    suffix_filter = " OR ".join([f"_TABLE_SUFFIX = '{suffix}'" for suffix in date_suffixes])
    
    query = f"""
    SELECT
      event_date,
      event_timestamp,
      event_name,
      user_pseudo_id,
      traffic_source.source AS traffic_source,
      traffic_source.medium AS traffic_medium,
      traffic_source.name AS traffic_name,
      geo.country AS geo_country,
      geo.region AS geo_region,
      geo.city AS geo_city,
      geo.sub_continent AS geo_sub_continent,
      geo.continent AS geo_continent,
      device.category AS device_category,
      device.operating_system AS device_os,
      device.web_info.browser AS browser,
      device.language AS device_language,
      ecommerce.transaction_id AS transaction_id,
      ecommerce.purchase_revenue_in_usd AS total_revenue_usd,
      ecommerce.total_item_quantity AS total_items,
      ep.key AS param_key,
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.float_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ) AS param_value,
      i.item_id AS item_id,
      i.item_name AS item_name,
      i.price_in_usd AS item_price_usd,
      i.quantity AS item_quantity
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS events
      LEFT JOIN UNNEST(events.event_params) AS ep
      LEFT JOIN UNNEST(events.items) AS i
    WHERE
      ({suffix_filter})
    AND event_name IN ('purchase', 'view_item', 'add_to_cart', 'page_view')
    """
    
    print(f"Querying data for {start_date} to {end_date}...")
    df_month = bq_client.query(query).to_dataframe(bqstorage_client=bq_storage_client)
    df_month.to_csv(f'exported_data_{start_date}.csv', index=False)
    #all_dfs.append(df_month)

# Concatenate all monthly DataFrames
#df = pd.concat(all_dfs, ignore_index=True)

# Save to CSV
#df.to_csv('exported_data.csv', index=False)

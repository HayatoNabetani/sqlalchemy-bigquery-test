from google.cloud import bigquery
from google.oauth2 import service_account


# ダウンロードした認証ファイル（.json）のフルパスを指定
key_path = "kashika-dpro-dev-0004.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# 実行クエリ
query = """
SELECT
  items.app_id AS app_id,
  apps.name as app_name,
  SUM(play_count_info.max_play_count) as play_count,
  SUM(play_count_info.min_play_count) as prev_play_count,
  SUM(play_count_info.play_count_difference * costs.price) AS cost_difference
FROM
  (
    SELECT
      statistics.item_id AS item_id,
      MAX(statistics.play_count) AS max_play_count,
      MIN(statistics.play_count) AS min_play_count,
      MAX(statistics.play_count) - MIN(statistics.play_count) AS play_count_difference
    FROM
      `kashika-dpro-dev-378004.dpro_test3.statistics` as statistics
    WHERE
      statistics.aggregation_time BETWEEN '2023-05-01 00:00:00' AND '2023-05-30 23:59:59'
    GROUP BY
      statistics.item_id
  ) AS play_count_info
  INNER JOIN
        `kashika-dpro-dev-378004.dpro_test3.items` as items
    ON  items.id = play_count_info.item_id
    AND items.parent_manage_id IS NULL
    AND items.media_type = 'VIDEO'
  INNER JOIN
        `kashika-dpro-dev-378004.dpro_test3.products` as products
    ON  items.product_id = products.id
  INNER JOIN
        `kashika-dpro-dev-378004.dpro_test3.apps` as apps
    ON  apps.id = items.app_id
    AND apps.id IN (2,3)
  INNER JOIN
        `kashika-dpro-dev-378004.dpro_test3.costs` as costs
    ON  costs.user_id = 'L0XxNs3oyUO2Pzij3TJ5e4vdOOA3'
    AND costs.app_id = items.app_id
    AND costs.genre_id = products.genre_id
    AND costs.media_type = items.media_type
Group By app_id, app_name
    """

# ジョブ実行
query_job = client.query(query)  # Make an API request.

results = query_job.result()

for result in results:
    print(result)

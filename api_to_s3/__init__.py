
from .parquet_io_manager import S3ParquetIOManager

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job
)

from api_to_s3.assets.hackernews import transform_hackernews_data,HackerNewsResource


daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="pipeline_job"), cron_schedule="0 0 * * *"
)


defs = Definitions(
    assets=[transform_hackernews_data],
    resources={
        "io_manager": S3ParquetIOManager(),
        "hackernews_resource": HackerNewsResource(base_url ="https://hacker-news.firebaseio.com/v0") 
    },
    schedules=[daily_refresh_schedule],
)

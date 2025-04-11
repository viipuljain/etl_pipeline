import pandas as pd
import requests
from dagster import MetadataValue, OpExecutionContext, asset, ConfigurableResource,Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, MetadataValue, TableSchema, TableColumn

class number_of_stories_to_transform(Config):
    number_of_stories: int = 500

class HackerNewsResource(ConfigurableResource):
    base_url : str
    def get_top_stories(self) :
        url = f"{self.base_url}/topstories.json"
        return requests.get(url).json()
    
    def get_story_details(self, item_id: int):
        url = f"{self.base_url}/item/{item_id}.json"
        return requests.get(url).json()

@asset(
    metadata={
        "dagster/column_schema": TableSchema(
            columns=[
                TableColumn("title", "string", description="Title of the Hacker News post"),
                TableColumn("by", "string", description="Username of the post author"),
                TableColumn("time", "timestamp", description="Time when the post was created"),
                TableColumn("score", "int", description="Score of the post"),
                TableColumn("num_comments", "int", description="Number of comments on the post"),
                TableColumn("url", "string", description="URL of the post"),
                TableColumn("domain", "string", description="Extracted domain from the URL"),
                TableColumn("engagement_ratio", "float", description="Score divided by number of comments"),
                TableColumn("title_length", "int", description="Length of the post title"),
                TableColumn("post_hour", "int", description="Hour of the day the post was made"),
                TableColumn("post_weekday", "string", description="Weekday the post was made"),
            ]
        )
    },
    tags = {"source":"hackernews_api","category": "news","priority":""},kinds = {"hackernewsapi","python"},description = "This Asset transform the hackernew api data to get the aggregated results. "
)
def transform_hackernews_data(
    context: OpExecutionContext, 
    hackernews_resource: HackerNewsResource,
    config :number_of_stories_to_transform
):
    top_stories = hackernews_resource.get_top_stories()[:config.number_of_stories]
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {executor.submit(hackernews_resource.get_story_details, item_id): item_id for item_id in top_stories}
        for future in as_completed(future_to_id):
            try:
                item = future.result()
                if item:
                    results.append(item)
                if len(results) % 20 == 0:
                    context.log.info(f"Fetched {len(results)} stories so far.")
            except Exception as e:
                context.log.error(f"Error fetching story {future_to_id[future]}: {e}")
    df = pd.DataFrame(results)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['kids'] = df['kids'].apply(lambda x: x if isinstance(x, list) else [])
    df['num_comments'] = df['kids'].apply(len)
    df['engagement_ratio'] = df['score'] / (df['num_comments'] + 1) 
    df['domain'] = df['url'].str.extract(r'://([^/]+)')
    df['title_length'] = df['title'].str.len()
    df['post_hour'] = df['time'].dt.hour
    df['post_weekday'] = df['time'].dt.day_name()
    
    return df[[
        'title',
        'by',
        'time',
        'score',
        'num_comments',
        'url',
        'domain',
        'engagement_ratio',
        'title_length',
        'post_hour',
        'post_weekday'
    ]].sort_values('score', ascending=False)
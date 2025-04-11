from dagster import ConfigurableIOManager, OutputContext,ResourceParam
import pandas as pd
import io
from datetime import datetime
import pyarrow   
import boto3  
import os


class S3ParquetIOManager(ConfigurableIOManager):
    aws_access_key: str = "aws_access_key"
    aws_secret_key: str = "aws_secret_key"
    bucket_name: str = "hacker-news-bucket"  
    def _get_s3_key(self):
        date = datetime.now()
        year, month, day = date.strftime('%Y'), date.strftime('%m'), date.strftime('%d')
        return f"hacker_news_data/year={year}/month={month}/day={year}-{month}-{day}/data.parquet"
    
    def _get_s3_client(self, context):
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key
            )
            s3_client.head_bucket(Bucket=self.bucket_name)
            return s3_client
        except Exception as e:
            context.log.error(f"Failed to connect to S3 or access bucket {self.bucket_name}: {str(e)}")
            return s3_client
    
    def handle_output(self, context: OutputContext, obj):
        try:
            if not isinstance(obj, pd.DataFrame):
                context.log.info(f"Pandas DataFrame not received.")       
            buffer = io.BytesIO()
            obj.to_parquet(buffer, engine="pyarrow")
            buffer.seek(0)
            
            s3_key = self._get_s3_key()
            s3_client = self._get_s3_client(context)     
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            context.log.info(f"Successfully saved Parquet file to s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            context.log.error(f"Unexpected error during S3 upload: {str(e)}")
    def load_input(self, context):
        pass


# class LocalCSVIOManager(ConfigurableIOManager):
#     def get_local_path(self):
#         date = datetime.now()
#         year, month, day = date.strftime('%Y'), date.strftime('%m'), date.strftime('%d')
#         folder_path = f"hacker_news_data/year={year}/month={month}/day={year}-{month}-{day}"
#         os.makedirs(folder_path, exist_ok=True)
#         return os.path.join(folder_path, "data.csv")
    
#     def handle_output(self, context: OutputContext, obj):
#         if not isinstance(obj, pd.DataFrame):
#             context.log.info(f"Pandas DataFrame not received.")     
        
#         file_path = self.get_local_path()
#         obj.to_csv(file_path, index=False)
#         context.log.info(f"Saved CSV file locally at {file_path}")
    
#     def load_input(self, context):
#         pass  
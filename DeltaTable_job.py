from pyspark.sql import SparkSession
import requests
import gzip
from pyspark.sql.types import StringType, LongType, TimestampType, IntegerType, DateType, StructField, StructType
import pyspark.sql.functions as F
import os

spark = SparkSession.builder \
    .appName('test-task-solution') \
    .master("local[*]") \
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:0.7.0') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.sql.session.timeZone', 'UTC') \
    .config('spark.databricks.delta.optimizeWrite.enabled', 'true') \
    .config('spark.databricks.delta.autoCompact.enabled', 'true') \
    .getOrCreate()

from delta.tables import *


events = ['CommitCommentEvent', 'PushEvent']




def get_schema(event):
    
    if event == 'CommitCommentEvent':
        
        schema_actor = StructType(
                [
                    StructField('id', LongType(), True),
                    StructField('login', StringType(), True),
                    StructField('gravatar_id', StringType(), True),
                    StructField('url', StringType(), True),
                    StructField('avatar_url', StringType(), True)
                ]
            )
    
        schema_repo = StructType(
                [
                    StructField('id', LongType(), True),
                    StructField('name', StringType(), True),
                    StructField('url', StringType(), True)
                ]
            )
    
        schema_payload_comment = StructType(
                [       
                    StructField('commit_id', StringType(), True),
                    StructField('body', StringType(), True)
                ]
            )

        schema_payload = StructType(
                [       
                    StructField('comment', schema_payload_comment, True)
                ]
            )

        schema_CommitCommentEvent = StructType(
                [       
                    StructField('id', StringType(), True),
                    StructField('type', StringType(), True),
                    StructField('actor', schema_actor, True),
                    StructField('repo', schema_repo, True),
                    StructField('created_at', TimestampType(), True),
                    StructField('payload', schema_payload, True),
                    StructField('dt', DateType(), True),
                    StructField('hr', IntegerType(), True)
                ]
            )
        return schema_CommitCommentEvent
    
    elif event == 'PushEvent':
        
        schema_actor = StructType(
                [
                    StructField('id', LongType(), True),
                    StructField('login', StringType(), True),
                    StructField('gravatar_id', StringType(), True),
                    StructField('url', StringType(), True),
                    StructField('avatar_url', StringType(), True)
                ]
            )
    
        schema_repo = StructType(
                [
                    StructField('id', LongType(), True),
                    StructField('name', StringType(), True),
                    StructField('url', StringType(), True)
                ]
            )
    
        schema_payload = StructType(
                [       
                    StructField('push_id', LongType(), True),
                    StructField('size', IntegerType(), True),
                    StructField('distinct_size', IntegerType(), True),
                    StructField('ref', StringType(), True),
                    StructField('head', StringType(), True),
                    StructField('before', StringType(), True)
                ]
            )

        schema_PushEvent = StructType(
                [       
                    StructField('id', StringType(), True),
                    StructField('type', StringType(), True),
                    StructField('actor', schema_actor, True),
                    StructField('repo', schema_repo, True),
                    StructField('created_at', TimestampType(), True),
                    StructField('payload', schema_payload, True),
                    StructField('dt', DateType(), True),
                    StructField('hr', IntegerType(), True)
                ]
            )
        return schema_PushEvent



def etl(dt: str, hr: int):
    print(f'Running ETL for dt: {dt} hr: {hr}')

    filename = dt + "-" + str(hr) + ".json"
    url = "https://data.gharchive.org/" + dt + "-" + str(hr) + ".json.gz"
    headers = {'User-agent': 'Chrome/84.0.4147.105'}
    
    try:
        response = requests.get(url, headers=headers)
        # TODO: Another way to save response directly into a file using `urllib.request.urlretrieve` function.
        with open(filename, 'wb') as f:            
            f.write(gzip.decompress(response.content))               
        
        for event in events:
            # TODO: No need to read full json for every event type, make it read file as text, extract type using
            # `get_json_object` function and then parse each event accorging to its type.
            df = spark.read.json(filename, schema=get_schema(event))
            
            df = df.filter(df.type == event) \
                .withColumn('dt', F.lit(dt).cast(DateType())) \
                .withColumn('hr', F.lit(hr).cast(IntegerType()))
            
            # TODO: Appending to table creates duplicates if function runs fwe times for same arguments.
            # Delta Lake supports `merge` operation which allows to upsert only new events.
            tbl_path = './tables/event_type=' + event
            df.write.partitionBy('dt').format("delta").mode("append").save(tbl_path)           
  
        os.remove(filename)
        
        print ('ETL complete')
    
    except Exception as e:       
        print ('ETL failed. Exception:', e)


def print_popular_commit_comments():
    print('Queiring CommitCommentEvent table')

    try:
        df = spark.read.format('delta').load('./tables/event_type=CommitCommentEvent')    
        df.select('payload.comment.body') \
        .groupBy('body') \
        .agg(F.count(F.lit(1)).alias("CommitCount")) \
        .where(F.col('CommitCount') > 1) \
        .orderBy('CommitCount', ascending=False) \
        .show(truncate=False, vertical=True)   
    
    except Exception as e:
        print ('Query failed. Exception:', e)


etl("2020-08-12", 7)
print_popular_commit_comments()


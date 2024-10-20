# Defining streaming message schema
import os

from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from config.conf import config_by_name
from utils.spark_helper import build_spark_session

schema = StructType([
    StructField('venue', StructType([
        StructField('venue_name', StringType()),
        StructField('lon', StringType()),
        StructField('lat', StringType()),
        StructField('venue_id', StringType())
    ])),
    StructField('visibility', StringType()),
    StructField('response', StringType()),
    StructField('guests', StringType()),
    StructField('member', StructType([
        StructField('member_id', StringType()),
        StructField('photo', StringType()),
        StructField('member_name', StringType())
    ])),
    StructField('rsvp_id', StringType()),
    StructField('mtime', StringType()),
    StructField('event', StructType([
        StructField('event_name', StringType()),
        StructField('event_id', StringType()),
        StructField('time', StringType()),
        StructField('event_url', StringType())
    ])),
    StructField('group', StructType([
        StructField('group_topics', ArrayType(StructType([
            StructField('urlkey', StringType()),
            StructField('topic_name', StringType())
        ]))),

        StructField('group_city', StringType()),
        StructField('group_country', StringType()),
        StructField('group_id', StringType()),
        StructField('group_name', StringType()),
        StructField('group_lon', StringType()),
        StructField('group_urlname', StringType()),
        StructField('group_lat', StringType())
    ]))
])

# Reading Kafka message
spark = build_spark_session()
kafka_df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', config_by_name[os.getenv("PLATFORM", "local")].KafkaServer)\
    .option('subscribe', config_by_name[os.getenv('PLATFORM', 'local')].KafkaTopic)        \
    .option('startingOffsets', 'latest') \
    .load()

# Get the (key, value) Kafka message, extract value and convert it to a dataframe with the defined schema
value_df = kafka_df.select(f.from_json(f.col('value').cast('string'), schema).alias('value'),
f.col('timestamp').cast('TIMESTAMP').alias('event_time'))
value_df = value_df.select('value.*', 'event_time')

# Explode value_df and select the fields from it
explod_df = value_df.selectExpr()

# Flatten group_topics
flatten_df = explod_df\
    .withColumn()\
    .withColumn()\
    .drop("group_topics")

# Find the response_count grouping by group_name, group_country, group_lat, group_lon, response
response_count_df = flatten_df.groupBy().agg()


# Write flatten_df into Mongodb using foreachBach() function
print("Job completed!!!")
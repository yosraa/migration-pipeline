# IAWS Glue tutorial
import os

from pyspark.sql import functions as f

from utils.aws_helper import build_client_service
from config.conf import config_by_name

# Initialize boto3 client for Glue
from utils.spark_helper import build_spark_session

glue_client = build_client_service("glue")

# Fetch the database list
response = glue_client.get_databases()

table_info = glue_client.get_table(
    DatabaseName=config_by_name[os.getenv("PLATFORM", "local")].DATABASE_name,
    Name=config_by_name[os.getenv("PLATFORM", "local")].TABLE_NAME
)
# Get the location of the data in S3
s3_path = response['Table']['StorageDescriptor']['Location']

#########################################
### EXTRACT (READ DATA)
#########################################

spark = build_spark_session()
df = spark.format('csv').option("header", "true").load(s3_path)

# create a decade column from year
decade_col = f.floor(df['year'] / 10) * 10
df = df.withColumn("decade", decade_col)
# Group by decade: Count movies, get average rating
data_frame_aggregated = df.groupby(df["decade"]).agg(f.count(df['movie_title']).alias('movie_count')
                                                     , f.mean(f.col("rating")).alias('rating_mean'))
# Sort by the number of movies per the decade
data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("movie_count"))
data_frame_aggregated.show(10)

#########################################
###   LOAD (WRITE DATA)
#########################################
# Create just 1 partition, because there is so little data
data_frame_aggregated = data_frame_aggregated.coalesce(1)

# Write data back to S3
data_frame_aggregated.write.format("csv").option("header", "true").save("s3a://your-bucket-name/path/to/destination/")

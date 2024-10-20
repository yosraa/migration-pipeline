from pyspark.sql import SparkSession


def build_spark_session():

    return SparkSession.builder.appName("migration-pipeline").master("local[2]").getOrCreate()

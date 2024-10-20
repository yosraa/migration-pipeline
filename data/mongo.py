import os

from config.conf import config_by_name


def mongo_sink(df):
    df.write \
        .format('mongo') \
        .mode('append') \
        .option('spark.mongodb.output.uri', config_by_name[os.getenv('PLATFORM', 'local')].MONGO_OUTPUT) \
        .save()
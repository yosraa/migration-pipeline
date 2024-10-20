import os

import boto3
from config.conf import config_by_name


def build_client_service(aws_service):
    if os.getenv("PLATFORM", "local") == "local":
        session = boto3.Session(profile_name="migration_pipeline")
        return session.client(aws_service, **config_by_name[os.getenv("PLATFORM", "local")].AWS_EXTRA.PARAMETERS)
    if os.getenv("AWS_REGION"):
        return boto3.client(aws_service, region_name=os.getenv("AWS_REGION"))
    return None

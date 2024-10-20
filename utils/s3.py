import boto3
from botocore.exceptions import NoCredentialsError

# Import python modules
import logging

from aws_helper import build_client_service

LOGGER = logging.getLogger(__name__)


def upload_file_to_s3(file_name, bucket, object_name=None):
    try:
        s3 = build_client_service('s3')
        if object_name is None:
            object_name = file_name
        s3.upload_file(file_name, bucket, object_name)
        LOGGER.info(f"Fichier {file_name} chargé avec succès dans {bucket}")
    except NoCredentialsError:
        LOGGER.error("Les informations d'identification AWS ne sont pas valides")

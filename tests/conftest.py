import pytest
from pyspark.sql import SparkSession


# ce fichier permet d'initialiser une session spark pour les tests, et de la fermer apres les tests

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest-pyspark-testing") \
        .getOrCreate()
    yield spark
    spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.functions import upper


def test_uppercase_transformation(spark):
    # Données d'entrée
    data = [("John", "Doe"), ("Jane", "Doe")]
    columns = ["first_name", "last_name"]

    input_df = spark.createDataFrame(data, columns)

    # df = input_df.withColumn('first_name', col('first_name'))
    # df.show()

    # transformation à tester
    def to_uppercase(df):
        return df.withColumn('first_name', upper(df['first_name']))

    result_df = to_uppercase(input_df)

    # Données attendues

    expected_data = [("JOHN", "Doe"), ("JANE", "Doe")]
    expected_data = spark.createDataFrame(expected_data, columns)

    assert result_df.collect() == expected_data.collect()

    def assert_df_equality(df1, df2):
        assert df1.collect() == df2.collect()

    def test_complex_transformation(spark):
        data = [
            ("John", "Doe", 30),
            ("Jane", "Doe", 25)
        ]
        columns = ["first_name", "last_name", "age"]
        input_data = spark.createDataFrame(data, columns)

        # transformation complexe à test
        def add_full_name(df):
            return df.withColumn("full_name",
                                 concat_ws(" ", df['first_name'], df['last_name']))

        result_df = add_full_name(input_data)

        # Données attendues
        expected_data = [
            ("John", "Doe", 30, "John Doe"),
            ("Jane", "Doe", 25, "Jane Doe")
        ]
        expected_columns = ["first_name", "last_name", "age", "full_name"]
        expected_df = spark.createDataFrame(expected_data, expected_columns)

        # Comparaison des résultats
        assert result_df.collect() == expected_df.collect()

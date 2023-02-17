import pytest
from pyspark.sql import SparkSession
from unittest.mock import Mock

@pytest.fixture
def generate_df():
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [[1,2,3],[4,5,6],[7,8,9]]
    df = spark.createDataFrame(data, 'a integer, b integer, c integer')
    return df


@pytest.fixture
def spark_mock():
    spark_mock = Mock()
    type(spark_mock).write = spark_mock
    type(spark_mock).read = spark_mock
    spark_mock.table.return_value = spark_mock
    spark_mock.format.return_value = spark_mock
    spark_mock.option.return_value = spark_mock
    spark_mock.mode.return_value = spark_mock
    spark_mock.save.return_value = None
    return spark_mock



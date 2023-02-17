import unittest
from unittest import mock
import pytest
import read_data


def test_fn(spark_mock, generate_df):
    spark_mock.load.return_value = generate_df

    df = read_data.fn(spark_mock)
    df.show()
    assert df.count() == 3

if __name__=="__main__":
    pytest.main()
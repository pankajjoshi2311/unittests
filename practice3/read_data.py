from pyspark.sql import SparkSession

def fn(spark):
    path = "C:\\1\\IMP\\CTS\\dataset\\data2.txt"
    df = spark.read.format("csv").load(path)
    return df


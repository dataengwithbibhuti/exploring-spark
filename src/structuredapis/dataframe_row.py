from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    data_row = [Row("John", "USA"), ("Pieter","Canada")]
    my_df = spark.createDataFrame(data_row).toDF("Name","Country")
    my_df.show()
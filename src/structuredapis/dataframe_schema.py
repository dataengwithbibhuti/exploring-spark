from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataFrameIntro").getOrCreate()
    data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
            [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
            [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
            [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
            [5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
            [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]]

    # Type-1 : Using DDL schema
    ddl_schema = "id STRING, firstName STRING, lastName STRING, blogUrl STRING, published STRING, hits INT, campaigns ARRAY<STRING>"
    data_schema = StructType([
        StructField("id", IntegerType())
        , StructField("firstName", StringType())
        , StructField("lastName", StringType())
        , StructField("blogUrl", StringType())
        , StructField("published", StringType())
        , StructField("hits", IntegerType())
        , StructField("campaigns", ArrayType(StringType()))
    ])
    my_df = spark.createDataFrame(data, data_schema)
    my_df_ddl = spark.createDataFrame(data, ddl_schema)
    my_df.show()
    my_df_ddl.show()

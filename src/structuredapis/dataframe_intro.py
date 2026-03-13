from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Testing Push
if __name__ == "__main__": 
    spark = SparkSession.builder.appName("DataFrameIntro").getOrCreate()
    flights_df = spark.read.csv("/Users/bapu/Public/SourceData/flights_data/2015-summary.csv", header=True, inferSchema=True)
    transformed_df = (flights_df.groupBy("DEST_COUNTRY_NAME")
                      .sum("count")
                      .withColumnRenamed("sum(count)","destination_total")
                      .sort(desc("destination_total"))
                      .limit(5))
    transformed_df.show()

    
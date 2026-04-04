from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataFrameIntro").getOrCreate()
    data = [("Brook", 20), ("Danny", 30), ("Jules", 40), ("TD", 35), ("Brook", 25), ("Danny", 40), ("Jules", 35)]
    my_df = spark.createDataFrame(data).toDF("Name", "Age")
    transformed_df = my_df.groupBy("Name").agg(avg("Age").alias("AverageAge"))
    transformed_df.show()
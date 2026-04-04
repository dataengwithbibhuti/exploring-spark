from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataFrameIntro").getOrCreate()
    data = [("Brook", 20), ("Danny", 30), ("Jules", 40), ("TD", 35), ("Brook", 25), ("Danny", 40), ("Jules", 35)]
    dataRdd = spark.sparkContext.parallelize(data)

    # Aggregate and compute Average

    # Problem with the lambda transformation in RDD is, it doesn't have any readability, it's cryptic, opaque and hard to read.
    # It doesn't communicate the intention.
    transformedRdd = (dataRdd.map(lambda x: (x[0], (x[1], 1)))
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                      .map(lambda x: (x[0], x[1][0] / x[1][1])))
    print(transformedRdd.collect())

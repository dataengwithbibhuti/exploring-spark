from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("DataFrameIntro")
             .getOrCreate())

    # Applying schema manually
    my_schema = StructType([
        StructField("DEST_COUNTRY_NAME", StringType(), False, metadata={"description": "Destination country name"})
        ,StructField("ORIGIN_COUNTRY_NAME", StringType(), False, metadata={"description": "Origin country name"})
        , StructField("count", LongType(), nullable=False, metadata={"description": "Flight counts"})
    ])

    flights_df = (spark
                  .read
                  .schema(my_schema)
                  .csv("/Users/bapu/Public/SourceData/flights_data/2015-summary.csv"
                       , header=True))

    flights_df.printSchema()

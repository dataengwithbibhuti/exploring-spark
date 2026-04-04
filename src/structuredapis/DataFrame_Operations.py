from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (SparkSession.builder
             .config("spark.sql.warehouse.dir", "/Users/bapu/Public/data_warehouse")
             .enableHiveSupport().getOrCreate())

    data_schema = StructType([
        StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)]
    )
    fire_df = spark.read.schema(data_schema).csv("/Users/bapu/Public/SourceData/fire_calls/Fire_Incidents_20260404.csv")

    # Get number of partitions
    partition_numbers = fire_df.rdd.getNumPartitions()

    # Get records in each partitions
    partition_records_df = (fire_df.groupBy(spark_partition_id().alias("partition_number")).agg(
        count("*").alias("record_count")))
    partition_records_df.show()

    # Check tables in catalog
    tables = spark.catalog.listTables()
    print(tables)

    # Check if table exists
    isTableExists = spark.catalog.tableExists("default.fire_incidents")
    print(isTableExists)

    if isTableExists:
        spark.sql("DROP TABLE default.fire_incidents")

    fire_df.write.format("parquet").mode("overwrite").saveAsTable("fire_incidents")

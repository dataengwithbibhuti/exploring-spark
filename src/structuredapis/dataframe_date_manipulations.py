from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Spark Date Manipulation")
             .getOrCreate())

    csv_schema = (
        "`Incident Number` INT,`Exposure Number` INT,`ID` INT,`Address` STRING,`Incident Date` STRING,`Call Number` INT,`Alarm DtTm` STRING,"
        "`Arrival DtTm` STRING,`Close DtTm` STRING,`City` STRING,`zipcode` STRING,`Battalion` STRING,`Station Area` STRING,`Box` STRING,"
        "`Suppression Units` STRING,`Suppression Personnel` STRING,`EMS Units` INT,`EMS Personnel` INT,`Other Units` STRING,"
        "`Other Personnel` STRING,`First Unit On Scene` STRING,`Estimated Property Loss` STRING,`Estimated Contents Loss` STRING,"
        "`Fire Fatalities` INT,`Fire Injuries` INT,`Civilian Fatalities` INT,`Civilian Injuries` INT,`Number of Alarms` INT,"
        "`Primary Situation` STRING,`Mutual Aid` STRING,`Action Taken Primary` STRING,`Action Taken Secondary` STRING,"
        "`Action Taken Other` STRING,`Detector Alerted Occupants` STRING,`Property Use` STRING,`Area of Fire Origin` STRING,"
        "`Ignition Cause` STRING,`Ignition Factor Primary` STRING,`Ignition Factor Secondary` STRING,`Heat Source` STRING,"
        "`Item First Ignited` STRING,`Human Factors Associated with Ignition` STRING,`Structure Type` STRING,`Structure Status` STRING,"
        "`Floor of Fire Origin` INT,`Fire Spread` STRING,`No Flame Spread` STRING,`Number of floors with minimum damage` INT,"
        "`Number of floors with significant damage` INT,`Number of floors with heavy damage` INT,`Number of floors with extreme damage` INT,"
        "`Detectors Present` STRING,`Detector Type` STRING,`Detector Operation` STRING,`Detector Effectiveness` STRING,"
        "`Detector Failure Reason` STRING,`Automatic Extinguishing System Present` STRING,`Automatic Extinguishing Sytem Type` STRING,"
        "`Automatic Extinguishing Sytem Perfomance` STRING,`Automatic Extinguishing Sytem Failure Reason` STRING,"
        "`Number of Sprinkler Heads Operating` INT,`Supervisor District` INT,`neighborhood_district` STRING,`point` STRING,"
        "`data_as_of` STRING,`data_loaded_at` STRING")

    fire_df = (spark
               .read
               .option("header", "true")
               .schema(csv_schema)
               .csv("/Users/bapu/Public/SourceData/fire_calls/Fire_Incidents_20260404.csv"))

    date_columns_df = fire_df.select("Incident Date"
                                     , "Alarm DtTm"
                                     , "Arrival DtTm"
                                     , "Close DtTm"
                                     , "data_as_of"
                                     , "data_loaded_at")
    date_columns_df.printSchema()
    date_columns_df.show(truncate=False)

    transformed_date_df = (date_columns_df
                           .withColumn("incident_date",to_date("Incident Date", "yyyy/MM/dd"))
                           .withColumn("alarm_date_time", to_timestamp("Alarm DtTm", "yyyy/MM/dd hh:mm:ss a"))
                           .withColumn("incident_date_year", year("incident_date"))
                           .withColumn("incident_date_month", month("incident_date"))
                           .withColumn("incident_date_day", day("incident_date"))
                           .withColumn("incident_date_add", date_add("incident_date",2))
                           .withColumn("incident_date_diff", date_diff("incident_date_add", "incident_date"))
                           .drop("Incident Date")
                           .drop("Alarm DtTm")
                           .drop("Arrival DtTm")
                           .drop("Close DtTm")
                           .drop("data_as_of")
                           .drop("data_loaded_at")
                           )
    transformed_date_df.printSchema()
    transformed_date_df.show(truncate=False)

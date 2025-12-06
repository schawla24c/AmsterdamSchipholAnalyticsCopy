# Databricks notebook source
from pyspark.sql import functions
from pyspark.sql.window import Window
#importing windows, functions for data curation

# COMMAND ----------


#creates a spark table for the enriched table
flights_enriched = spark.table("amsterdam_enriched_catalog.enriched_data.flights_enriched")

#creates a new column known as traffic_time which gets the time of traffic at any part of the day by comparing flightDirecton, actualLandingTime and estimatedLandingTime.
flights_ground_time = (
    flights_enriched
    #adds a new column called traffic time to the dataframe
    .withColumn(
        "traffic_time",
        #conditions set for traffic_time, where flight direction must equal "A" or Arrival, and actual landing time and estimated landing time is not null, if neither its scheduled ladning time. 
        functions.when(
            (functions.col("flightDirection") == "A") & functions.col("actualLandingTime").isNotNull(),
            functions.col("actualLandingTime")
        )
        .when(
            (functions.col("flightDirection") == "A") & functions.col("estimatedLandingTime").isNotNull(),
            functions.col("estimatedLandingTime")
        )
        .otherwise(functions.col("scheduleDateTime"))
    )
)

#Creates a new column called traffic_hour by extracting the hour from traffic_time. The value from traffic_hour is then used in window functions to group flights by scheduleDate, traffic_hour and terminal. Lastly, utilizing the window functions, we generate two new columns which calculates the number of flights in the terminal and number of flights at airport at any given time. 
flights_ground_time = flights_ground_time.withColumn(
    "traffic_hour",
    functions.hour(functions.col("traffic_time"))
)

window_airport = Window.partitionBy("scheduleDate", "traffic_hour")
window_terminal = Window.partitionBy("scheduleDate", "traffic_hour", "terminal")

flights_congestion = (
    flights_ground_time
    .withColumn(
        "no_flights_in_airport",
        functions.count("*").over(window_airport)
    )
    .withColumn(
        "no_flights_in_terminal",
        functions.count("*").over(window_terminal)
    )
)

window_day = Window.partitionBy("scheduleDate")


#This code makes another column called congestion index which measures the congestion index which is the number of flights in the airport divided by the maximum number of flights in the airport for the day.
flights_congestion = flights_congestion.withColumn(
    "congestion_index",
    functions.col("no_flights_in_airport") / functions.max("no_flights_in_airport").over(window_day)
)



#Adds 3 columns. Creates a temporary time stamp column that uses actualLandingTime, estimatedLandingTime, and scheduleDateTime. This temporary column determines catches the best available time the flight happened based off the 3 columns, if neither of the first two columns are present, the value is parsed as time stamp using scheduleDateTime. Next it creates an official column known as delay minutes which measures the delay in minutes by subtracting the scheduled time from the tempoary flight event time and then dividing by 60. Flight_event_time formats the temporary time stamp to be consistent with the format I set for other columns in table (for ex: 2025-11-16 00:00) under a new column name that displays it in the table. Lastly the punctiality status flags the flight as cancelled, diverted, late, delayed, early or ontime based on set conditions from the delay minutes column and publicFlightStatus column.
flights_congestion = (
    flights_congestion
    .withColumn(
        "flight_event_stamp",
        functions.when(
            functions.col("actualLandingTime").isNotNull(),
            functions.to_timestamp("actualLandingTime", "yyyy-MM-dd HH:mm")
        )
        .when(
            functions.col("estimatedLandingTime").isNotNull(),
            functions.to_timestamp("estimatedLandingTime", "yyyy-MM-dd HH:mm")
        )
        .otherwise(
            functions.to_timestamp("scheduleDateTime", "yyyy-MM-dd HH:mm")
        )
    )
    .withColumn(
        "delay_minutes",
        (
            (
                functions.col("flight_event_stamp").cast("long")
                - functions.to_timestamp("scheduleDateTime", "yyyy-MM-dd HH:mm").cast("long")
            ) / 60
        ).cast("int")
    )
    .withColumn(
        "flight_event_time",
        functions.date_format(functions.col("flight_event_stamp"), "yyyy-MM-dd HH:mm")
    )
    .withColumn(
        "flight_punctuality_status",
        functions.when(
            functions.col("PublicFlightStates_abbrev").contains("CNX"),
            functions.lit("Cancelled")
        )
        .when(
            functions.col("PublicFlightStates_abbrev").contains("DIV"),
            functions.lit("Diverted")
        )
        .when(
            functions.col("delay_minutes") >= 40,
            functions.lit("Late")
        )
        .when(
            (functions.col("delay_minutes") >= 1) &
            (functions.col("delay_minutes") < 40),
            functions.lit("Delayed")
        )
        .when(
            functions.col("delay_minutes") <= -1,
            functions.lit("Early")
        )
        .otherwise(functions.lit("OnTime"))
    )
    .drop("flight_event_stamp")
)





#optional in case of debuging error or displaying the data
#display(flights_congestion)


#Writes the changes in the dataframe to the curated table 
flights_congestion.write.mode("overwrite").saveAsTable(
    "amsterdam_curated.curated_data.flights_curated"
)


# COMMAND ----------

#Pulling up the enriched table for aircraft Type
aircraftType_df = spark.table("amsterdam_enriched_catalog.enriched_data.aircraftType_enriched")

#Writes the dataframe to the curated table since its clean and saves the latest data to that table.  
aircraftType_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_curated.curated_data.aircraftType_curated"
)

# COMMAND ----------

#Pulling up the enriched table for airlines
airlines_df = spark.table("amsterdam_enriched_catalog.enriched_data.airlines_enriched")

#Writes the dataframe to the curated table since its clean and saves the latest data to that table.  
airlines_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_curated.curated_data.airlines_curated"
)

# COMMAND ----------

#Pulling up the enriched table for airlines
destinations_df = spark.table("amsterdam_enriched_catalog.enriched_data.destinations_enriched")

#Writes the dataframe to the curated table since its clean and saves the latest data to that table.  
destinations_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_curated.curated_data.destinations_curated"
)
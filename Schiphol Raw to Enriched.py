# Databricks notebook source
from pyspark.sql import functions
#importing our key needed aspects in order for data cleaning

# COMMAND ----------

#Pulls the raw destination data and converts it into a dataframe
destination_raw_df = spark.table("amsterdam_catalog_raw.destinations_raw.destinations")


#Parses publicName string column into a STRUCT to allow for separation of dutch and english names
destination_df = destination_raw_df.withColumn(
    "publicName_struct",
    functions.from_json(
        functions.col("publicName"),
        "STRUCT<dutch:STRING,english:STRING>"
    )
)

# Cleans country names with specific replacements to match the enriched data as some country names were a little bit unorganized. 
country_clean_name = functions.trim(functions.col("country"))
country_clean_name = functions.when(
    country_clean_name == "French Polynes", "French Polynesia"
).when(
    country_clean_name == "United Arab Emirates (the)United Arab Emirates (the)", "United Arab Emirates"
).when(
    country_clean_name == "Surinam", "Suriname"
).otherwise(country_clean_name)


#Create a list of transformations to apply to the dataframe including separating dutch names from english names
transformations = [
    ("country", country_clean_name),
    ("iata", functions.upper(functions.trim(functions.col("iata")))),
    ("publicName_dutch", functions.trim(functions.col("publicName_struct.dutch"))),
    ("publicName_english", functions.trim(functions.col("publicName_struct.english")))
]

#Apply transformations to the dataframe via a for loop
for column_name, changes in transformations:
    destination_df = destination_df.withColumn(column_name, changes)


#specifies the columns to be kept
destination_df = destination_df.select("country", "iata", "publicName_dutch", "publicName_english")




#Writes the dataframe to the enriched table and saves the changes with the latest but cleaned data. 
destination_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_enriched_catalog.enriched_data.destinations_enriched"
)


#optional display in case data frame needs to be displayed for error debugging or data issues
#display(destination_df)

# COMMAND ----------


#declares a list of timestamps in the data to be cleaned up
timestamp_conversion = [
 "actualLandingTime", 
 "lastUpdatedAt", 
 "estimatedLandingTime", 
 "expectedTimeOnBelt", 
 "scheduleDateTime"
]

#Load the raw flight table
flights_raw_df = spark.table("amsterdam_catalog_raw.flights_raw.flights")

#sets our clean up table equal to the raw spark table
clean_flights_df = flights_raw_df

#Converts the timestamps in the list to a readable format
for conversion in timestamp_conversion:
    clean_flights_df = clean_flights_df.withColumn(conversion, functions.to_timestamp(conversion))

#For each conversion in the list, we set it equal to the year, month, day and the hours and minutes.
for conversion in timestamp_conversion:
    clean_flights_df = clean_flights_df.withColumn(conversion, functions.date_format(functions.col(conversion), "yyyy-MM-dd HH:mm"))


#Parses the aircraftType column into a STRUCT by replacing with double quotes and then using from_json to convert with iataMain and iataSub.
clean_flights_df = clean_flights_df.withColumn(
    "aircraftType",
    functions.from_json(
        functions.translate(functions.col("aircraftType"), "'", '"'),
        "iataMain STRING, iataSub STRING"
    )
)

#Pull out the two fields as separate valyes and then we drop aircraftType
clean_flights_df = clean_flights_df \
    .withColumn("iataMain", functions.col("aircraftType.iataMain")) \
    .withColumn("iataSub",  functions.col("aircraftType.iataSub")) \
    .drop("aircraftType")





#Cocentates abbreviated publicFightState values into a single string, separated by commas, and then joins the result in one string. Demonstrates the cleaned up abbreviated versions. shows flight states that contain scheduled, expectation updated, airborne, arrival, landed, departed, cancelled, boarding, gate changed, guaranteed departure, filed but not active, flight in radar.
public_flight_states_clean = functions.concat_ws(
    ", ",
    functions.array(
        functions.when(functions.col("publicFlightState")["flightStates"].contains("SCH"), functions.lit("SCH")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("EXP"), functions.lit("EXP")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("AIR"), functions.lit("AIR")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("ARR"), functions.lit("ARR")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("LND"), functions.lit("LND")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("DEP"), functions.lit("DEP")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("CNX"), functions.lit("CNX")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("BRD"), functions.lit("BRD")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("GCH"), functions.lit("GCH")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("GTD"), functions.lit("GTD")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("FIB"), functions.lit("FIB")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("FIR"), functions.lit("FIR"))
    )
)

#Cocentates the expanded publicFightState values into a single string, separated by commas, and then joins the result in one string. Demonstrates the cleaned up expanded versions. 
public_flight_states_expanded_clean = functions.concat_ws(
    ", ",
    functions.array(
        functions.when(functions.col("publicFlightState")["flightStates"].contains("SCH"), functions.lit("Scheduled")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("EXP"), functions.lit("Expectation Updated")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("AIR"), functions.lit("Airborne")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("ARR"), functions.lit("Arrival")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("LND"), functions.lit("Landed")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("DEP"), functions.lit("Departed")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("CNX"), functions.lit("Cancelled")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("BRD"), functions.lit("Boarding")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("GCH"), functions.lit("Gate Changed")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("GTD"), functions.lit("Guaranteed Departure")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("FIB"), functions.lit("Filed But Not Active")),
        functions.when(functions.col("publicFlightState")["flightStates"].contains("FIR"), functions.lit("Flight In Radar"))
    )
)


#Declares two new columns, the abbreviated and expanded publicFlightState values
clean_flights_df = clean_flights_df.withColumn(
    "PublicFlightStates_abbrev",
    public_flight_states_clean
).withColumn(
    "PublicFlightStates_exp",
    public_flight_states_expanded_clean
).drop("publicFlightState")

#Cleans up baggageClaim from its raw state to ensure that if the belts in the map is not null, it sets it to the belt number (example is belt 17), and if there is no number, it sets it to null rather than just belt belt
clean_flights_df = clean_flights_df.withColumn(
    "baggageClaim",
    functions.when(
        functions.col("baggageClaim")["belts"].isNotNull(),
        functions.concat_ws(
            " ",
            functions.lit("Belt"),
            functions.translate(functions.col("baggageClaim")["belts"], "[]'", "")
        )
    )
)

#Breaks the raw data of the route column and creates separate new columns known as IsIn_EU, Visa_Required and destination_route. These values were all apart of routes in the raw data. Then it organizes IsIn_EU based on research knowledge regarding the meanings of abbreviations, and classifies them accordingly. Visa_Required is then used as a true and false boolean based on the original data in the raw version (like if it says false in the raw data, then it will say raw but in its own column rather than in the same column with route). We also create a new column called destination_route which is formatted without the brackets in the raw data. Next, we add several conditions to demonstrate the direction the flights are coming from, referring to the flight states from both the abbreviated and expanded versions as conditions based off the vales. If the flight is arriving, it will be demonstrating in route as LPA -> AMS, and if its departing, boarding or other noted conditions, it will be demonstrating in route as AMS -> LPA. This code also accounts for the multi destination where it has multiple destinations, and it will be demonstrating in route as AMS -> LPA -> JFK for example. Lastly, we drop the original route column as it is no longer needed.
clean_flights_df = clean_flights_df \
    .withColumn(
        "IsIn_EU",
        functions.when(functions.col("route")["eu"] == functions.lit("S"),
               functions.lit("In EU, Schengen area")
        ).when(functions.col("route")["eu"] == functions.lit("E"),
               functions.lit("In EU, not in Schengen area")
        ).when(functions.col("route")["eu"] == functions.lit("N"),
               functions.lit("Non-Schengen area")
        ).when(functions.col("route")["eu"] == functions.lit("O"),
               functions.lit("Outside EU")
        ).otherwise(functions.lit("Unknown EU or Schengen status"))
    ) \
    .withColumn(
        "Visa_Required",
        (functions.col("route")["visa"] == functions.lit("true"))
    ) \
    .withColumn(
        "destination_route",
        functions.translate(functions.col("route")["destinations"], "[]'", "")
    )
clean_flights_df = clean_flights_df.drop("route")


departure_true = (
    (functions.col("flightDirection") == functions.lit("D")) |
    functions.col("PublicFlightStates_abbrev").contains("AIR") |
    functions.col("PublicFlightStates_abbrev").contains("DEP") |
    functions.col("PublicFlightStates_abbrev").contains("GTD") |
    functions.col("PublicFlightStates_abbrev").contains("BRD") |
    functions.col("PublicFlightStates_abbrev").contains("FIB") |
    functions.col("PublicFlightStates_abbrev").contains("FIR") |
    functions.col("PublicFlightStates_exp").contains("Airborne") |
    functions.col("PublicFlightStates_exp").contains("Departed") |
    functions.col("PublicFlightStates_exp").contains("Guaranteed Departure") |
    functions.col("PublicFlightStates_exp").contains("Boarding") |
    functions.col("PublicFlightStates_exp").contains("Filed But Not Active") |
    functions.col("PublicFlightStates_exp").contains("Flight In Radar")
)

arrival_true = (
    (functions.col("flightDirection") == functions.lit("A")) |
    functions.col("PublicFlightStates_abbrev").contains("ARR") |
    functions.col("PublicFlightStates_abbrev").contains("LND") |
    functions.col("PublicFlightStates_exp").contains("Arrival") |
    functions.col("PublicFlightStates_exp").contains("Landed")
)



destination_clean = functions.concat_ws(
    " -> ",
    functions.split(functions.col("destination_route"), ", ")
)

clean_flights_df = clean_flights_df.withColumn(
    "destination_route",
    functions.when(
        departure_true & ~functions.col("PublicFlightStates_abbrev").contains("CNX"),
        functions.concat(
            functions.lit("AMS -> "),
            destination_clean
        )
    ).when(
        arrival_true & ~functions.col("PublicFlightStates_abbrev").contains("CNX"),
        functions.concat(
            destination_clean,
            functions.lit(" -> AMS")
        )
    ).otherwise(
        functions.concat(
            functions.lit("AMS -> "),
            destination_clean
        )
    )
)

#Cleans up Schedule time to just display the hour and minute only rather than both as well as seconds. 
clean_flights_df = clean_flights_df.withColumn(
    "scheduleTime",
    functions.date_format(
        functions.to_timestamp(functions.col("scheduleTime"), "HH:mm:ss"),
        "HH:mm"
    )
)


#specifies the columns to be kept
clean_flights_df = clean_flights_df.select("id", "lastUpdatedAt", "flightName", "mainFlight", "flightNumber", "scheduleDate", "scheduleTime", "scheduleDateTime", "flightDirection", "destination_route", "PublicFlightStates_abbrev", "PublicFlightStates_exp", "estimatedLandingTime", "actualLandingTime", "expectedTimeOnBelt", "terminal", "baggageClaim", "prefixIATA", "prefixICAO", "airlineCode", "serviceType", "iataMain", "iataSub", "isOperationalFlight", "schemaVersion", "IsIn_EU", "Visa_Required")



#Writes the dataframe to the enriched table and saves the changes with the latest but cleaned data. 
clean_flights_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_enriched_catalog.enriched_data.flights_enriched"
)


#Uncomment this if you want to see dataset/debug any issues.
#display(clean_flights_df)

# COMMAND ----------

#Pulling up the raw table for aircraftType
aircraft_Type_df = spark.table("amsterdam_catalog_raw.aircrafttypes_raw.aircrafttypes")

#Writes the dataframe to the enriched table since its clean and saves the latest data to that table.  
aircraft_Type_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_enriched_catalog.enriched_data.aircraftType_enriched"
)

# COMMAND ----------

#Pulling up the raw table for airlines
airlines_df = spark.table("amsterdam_catalog_raw.airlines_raw.airlines")

#Writes the dataframe to the enriched table since its clean and saves the latest data to that table.  
airlines_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "amsterdam_enriched_catalog.enriched_data.airlines_enriched"
)
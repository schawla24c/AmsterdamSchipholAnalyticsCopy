# Databricks notebook source
import os
import requests
import json
from pyspark.sql import Row, functions, types
from datetime import datetime, timedelta
import json
#declares imports needed for ingestion

# COMMAND ----------

#defines main url we will be pulling api data from
main_url = "https://api.schiphol.nl/public-flights/"
#calls the Schiphol api and returns json response
header = {
    "Accept": "application/json",
    "app_id": "a7d8cafa",
    "app_key": "65143baf195a65afd30450373a17b9dc",
    "ResourceVersion": "v4"
 }

#function created to send a GET request to the API end point with appropriate authentication headers. 
def retrieve_api(endpoint: str, params: dict = None):
    try:
        response = requests.get(f"{main_url}{endpoint}", headers=header, params=params)
        response.raise_for_status()
        return response.json()
    #exception created so that API returns json, else if there is an error it prints it rather than breaking code. 
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

#declares the target endpoints based off API documentation
endpoints = [
    "flights",
    "airlines",
    "aircrafttypes",
    "destinations"
]

#amsterdam_catalog_raw is already a created catalog, so we are selecting/updating the catalog here
#create new schemas within catalog 
spark.sql("USE CATALOG amsterdam_catalog_raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS flights_raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS airlines_raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS aircrafttypes_raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS destinations_raw")


#declares the structure of the flights schema including the columns within it in accordance with Schiphol API Documentation
flights_schema = types.StructType([
    types.StructField("lastUpdatedAt", types.StringType(),  True),
    types.StructField("actualLandingTime", types.StringType(),  True),
    types.StructField("aircraftType", types.StringType(),  True),
    types.StructField("baggageClaim", types.MapType(types.StringType(), types.StringType()), True),
    types.StructField("estimatedLandingTime", types.StringType(),  True),
    types.StructField("expectedTimeOnBelt", types.StringType(),  True),
    types.StructField("flightDirection", types.StringType(),  True),
    types.StructField("flightName", types.StringType(),  True),
    types.StructField("flightNumber", types.IntegerType(), True),
    types.StructField("id", types.StringType(),  True),
    types.StructField("isOperationalFlight", types.BooleanType(), True),
    types.StructField("mainFlight", types.StringType(),  True),
    types.StructField("prefixIATA", types.StringType(),  True),
    types.StructField("prefixICAO", types.StringType(),  True),
    types.StructField("airlineCode", types.IntegerType(), True),
    types.StructField("publicFlightState", types.MapType(types.StringType(), types.StringType()), True),
    types.StructField("route", types.MapType(types.StringType(), types.StringType()), True),
    types.StructField("scheduleDateTime", types.StringType(),  True),
    types.StructField("scheduleDate", types.StringType(),  True),
    types.StructField("scheduleTime", types.StringType(),  True),
    types.StructField("serviceType", types.StringType(),  True),
    types.StructField("terminal", types.IntegerType(), True),
    types.StructField("schemaVersion", types.StringType(),  True),
])

#Calls the retrieve API function to retrieve the data from the API and creates a Spark DataFrame from the retrieved data for storage in catalog
flight_raw_json = retrieve_api("flights", params={"page": 0})
if flight_raw_json and "flights" in flight_raw_json:
    flight_raw_df = spark.createDataFrame(flight_raw_json["flights"], flights_schema)
    flight_raw_df.write.mode("append").option("mergeSchema", "true").saveAsTable("amsterdam_catalog_raw.flights_raw.flights")


#declares the structure of the aircraftypes schema including the columns within it in accordance with Schiphol API Documentation
aircrafttypes_schema = types.StructType([
    types.StructField("iataMain", types.StringType(), True),
    types.StructField("iataSub", types.StringType(), True),
    types.StructField("longDescription", types.StringType(), True),
    types.StructField("shortDescription", types.StringType(), True),
])


#Calls the retrieve API function to retrieve the data from the API and creates a Spark DataFrame from the retrieved data for storage in catalog. This schema is given an conditional check to avoid code breaking. This will be a defensive check to ensure that 'aircraftTypes' exists in the response, else if no data is returned or authentication fails, a message is printed rather than breaking the process.
aircraftType_raw_json = retrieve_api("aircrafttypes", params={"page": 0})
if aircraftType_raw_json and "aircraftTypes" in aircraftType_raw_json:
    aircraftType_raw_df = spark.createDataFrame(aircraftType_raw_json["aircraftTypes"], schema=aircrafttypes_schema)
    aircraftType_raw_df.write.mode("append").option("mergeSchema", "true").saveAsTable("amsterdam_catalog_raw.aircrafttypes_raw.aircrafttypes")
else:
    print("No data found for aircrafttypes.")


#declares the structure of the airlines schema including the columns within it in accordance with Schiphol API Documentation
airlines_schema = types.StructType([
    types.StructField("iata", types.StringType(), True),
    types.StructField("icao", types.StringType(), True),
    types.StructField("nvls", types.IntegerType(), True),
    types.StructField("publicName", types.StringType(), True),
])


#Calls the retrieve API function to retrieve the data from the API and creates a Spark DataFrame from the retrieved data for storage in catalog
airline_raw_json = retrieve_api("airlines", params={"page": 0})
if airline_raw_json and "airlines" in airline_raw_json:
    airline_raw_df = spark.createDataFrame(airline_raw_json["airlines"], airlines_schema)
    airline_raw_df.write.mode("append").option("mergeSchema", "true").saveAsTable("amsterdam_catalog_raw.airlines_raw.airlines")


#declares the structure of the destination schema including the columns within it in accordance with Schiphol API Documentation
destination_schema = types.StructType([
    types.StructField("country", types.StringType(), True),
    types.StructField("iata", types.StringType(), True),
    types.StructField("publicName", types.StringType(), True),
])


#Calls the retrieve API function to retrieve the data from the API and creates a Spark DataFrame from the retrieved data for storage in catalog
destination_raw_json = retrieve_api("destinations", params={"page": 0})
if destination_raw_json and "destinations" in destination_raw_json:
    destination_raw_df = spark.createDataFrame(destination_raw_json["destinations"], destination_schema)
    destination_raw_df.write.mode("append").option("mergeSchema", "true").saveAsTable("amsterdam_catalog_raw.destinations_raw.destinations")
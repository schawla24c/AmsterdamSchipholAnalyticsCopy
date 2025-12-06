# Data Trends and Operational Insights at Amsterdam Schiphol Airport
An project pipeline using Databricks, PowerBI and APIs.

# Description
Databricks pipeline designed to ingest and clean data from Raw to Curated from Amsterdam's Schiphol Public Flights API (v4). The data from this Pipeline is then connected in Power BI via its many source connections from Power BI itself. 

# Objective
This project will extract data from a public API and build an end-to-end data pipeline following the raw to enriched to curated architecture. Incoming data is refreshed by the API every 10 minutes. Leveraging Databricks Free Edition features, the pipeline will ingest raw data every 12 minutes into the raw catalog. Four minutes after ingestion, an enrichment job transforms and cleans the data, preparing it for curated updates. Six minutes later, the curated job creates business-suitable tables with additional columns, calculations, and appropriate formatting. The final curated dataset will support business machine learning pipeline, which will then conduct EDA as well as answer key framing questions designated by the business on a hourly basis.

# Dataset
Amsterdam Schiphol’s Public Flight (v4) API provides free, continually updated flight records in batches of 20 records every 10 minutes. The focus tables for the project are flights, aircraftType, destinations, and airlines. The flights table contains core information about flights scheduled, arriving, departing, or currently in Schiphol’s airspace as well as when it was last updated, the terminal, the baggage claim belt, the direction, the state of the flight, and so forth. It serves as the central fact table, linking to aircraftType, destinations, and airlines through foreign keys. The aircraftType table describes the aircraft model used for each flight and includes identifiers such as the IATA aircraft type code. The destinations table identifies the origin or destination country associated with each flight. Finally, the airlines table contains information about the carriers operating these flights.

# Technologies Used: 
- Databricks Free Edition - [Sign up for Databricks Free Tier](https://login.databricks.com/signup?intent=SIGN_UP&provider=DB_FREE_TIER&tuuid=6587dae8-cff2-4a5e-b1c7-0436a4607885&dbx_source=direct&rl_aid=14a6dde2-75b5-4e9b-b60f-513f6fd4efa7&sisu_state=eyJsZWdhbFRleHRTZWVuIjp7Ii9zaWdudXAiOnsidG9zIjp0cnVlLCJwcml2YWN5Ijp0cnVlLCJjb3Jwb3JhdGVFbWFpbFNoYXJpbmciOnRydWV9fX0%3D)
- Schiphol Public Flights API - [Sign up for the Schiphol API (Free)](https://developer.schiphol.nl/)
- Power BI - [Sign up for Power BI and download the Free version, use your vt email](https://app.powerbi.com/)
- Powerpoint for Infographics - [PowerPoint download or use vt email](https://powerpoint.cloud.microsoft/) 


# Dashboard Results
<img width="1163" height="673" alt="schiphol 3" src="https://github.com/user-attachments/assets/ea26b880-bec4-44e0-b159-8af50e185566" />
<img width="1222" height="716" alt="Schiphol 2" src="https://github.com/user-attachments/assets/a06f7d0b-c591-4326-aab6-ed21bfe17894" />
<img width="1147" height="661" alt="schiphol 4" src="https://github.com/user-attachments/assets/fddec1e2-5819-4f04-9d6e-635d8047de33" />
<img width="1177" height="691" alt="Schiphol 1" src="https://github.com/user-attachments/assets/a72b842d-4709-4051-acb1-a72a661e5d5a" />


# Infographic
<img width="1235" height="697" alt="Screenshot 2025-12-05 223147" src="https://github.com/user-attachments/assets/e3ac838c-5f69-48a0-905c-28dd425dde60" />


# Data Quality Assessment: Describe the quality status of the data set and the way you assessed it

Across the four API tables, the data quality was ideal for airlines and aircraftType. As a result no cleaning or modifications were rendered. Most of the work occurred in the destinations and flights tables. Several fields required modifications, such as routes in flights as well as publicName in destinations, for example. Both arrived in nested {} formats or long concatenated strings that were not ideal for business use cases. These fields required restructuring into cleaner strings or timestamp formats and, in some cases, splitting into multiple columns. For example, routes in flights was broken into visa required, IsInEu, and routes. Numerous timestamps in the flights table also needed parsing to standardize the data and ensure consistency across the database. Once these corrections were applied, along with others not mentioned, the curated tables were ready for business use case application.



# Conclusion

The data yielded interesting insights to trends at Schiphol Airport. Through the data, it was observed that the worst day to travel on any day of the week fell on a Tuesday, as flights tended to be delayed 37% of the time. Friday turned out to be the perfect day for travel as around 80% of the flights tended to be early. Flights on the ground was a difficult measure to derive, and typically involved utilizing a mix of scheduled date, actual landing time as well as estimated landing time. This was a challenge as it would have been helpful to have the actual departure time as well as more informationa about when the flight left the ground. Additionally, weather data along with taxiway ques, gate availability, and holding patterns would have allowed for a more accurate ground congestion index. Pipeline limitations were few in between but to import the data to a csv excel file was deemed difficult and as such took advantage of Power BIs connection to the databricks connector. Otherwise everything was perfect as is. A future step for this project would be to ideally to utilize diverse APIs related to this project as relying on one API, although satisfactory for scale, would not be sufficient for a data engineering project focusing on improvements as well as reliance on live data to make data driven decisions. Ultimately, this project could have been improved with relying on not one, but many APIs. 









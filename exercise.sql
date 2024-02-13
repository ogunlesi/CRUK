import os
import pandas as pd
pip install parquet-cli 
print(os.getcwd())
os.chdir("/Users/Mac/documents")
taxi_data = pd.read_parquet("yellow_tripdata_2021-03.parquet")

# Preview first 5 rows
taxi_data.head() 

# Select necessary columns
selected_columns = ["trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"]
df = taxi_data[selected_columns]

# Correct data quality issue
df.loc[taxi_data['PULocationID'] == 161, 'PULocationID'] = 237
df.loc[taxi_data['PULocationID'] == 237, 'PULocationID'] = 161

#  updated data to a new Parquet file
df.to_parquet("yellow_tripdata_2021-03_updatedv1.parquet")

#  bringing updated data into dataframe 
taxi_data_updated = pd.read_parquet("yellow_tripdata_2021-03_updatedv1.parquet")

  
# python for average speed
# Calculate total distance travelled in miles
total_distance = taxi_data_updated['trip_distance'].sum()

# Calculate total time taken in seconds
total_time = (taxi_data_updated['tpep_dropoff_datetime'] - taxi_data_updated['tpep_pickup_datetime']).dt.total_seconds().sum()

# Convert total time to hours
total_time_hours = total_time / 3600

# Calculate average speed in miles per hour
average_speed = total_distance / total_time_hours
print("Average speed in March 2021:", average_speed, "miles per hour")

# Calculate speed for each trip
taxi_data_updated['speed'] = taxi_data_updated['trip_distance'] / ((taxi_data_updated['tpep_dropoff_datetime'] - taxi_data_updated['tpep_pickup_datetime']).dt.total_seconds() / 3600)

# Calculate mean speed for all trips in March 2021
mean_speed = taxi_data_updated['speed'].mean()

# Filter trips slower than the average speed
slow_trips = taxi_data_updated[taxi_data_updated['speed'] < mean_speed]

# Group by week and Pick up locations, count occurrences, and sort
top_pickup_locations = slow_trips.groupby([pd.Grouper(key='tpep_pickup_datetime'), 'PULocationID']).size().reset_index(name='count').sort_values(by='count', ascending=False)

# Extract top 5 pick-up locations for each week
top_pickup_locations_grouped = top_pickup_locations.groupby(pd.Grouper(key='tpep_pickup_datetime')).head(5)

print("Top 5 Pick-up locations for each week resulting in trips slower than the average speed:")
print(top_pickup_locations_grouped)
  
SQL for average speed
--- 60 mins in an hour & 60 seconds in a munutes 60x60 g
SELECT AVG(trip_distance / TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND)) * 3600 AS avg_speed_mph
FROM data
WHERE trip_distance > 0
  AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) > 0;

SQL for Top 5 pick up locations with slower trips
SELECT PULocationID, AVG(speed_mph) AS avg_speed_mph
FROM (
    SELECT PULocationID, 
           trip_distance / TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) * 3600 AS speed_mph
    FROM taxi_data_updated -- Using the updated dataset
    WHERE trip_distance > 0
      AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) > 0
) AS trip_speeds
WHERE AVG(speed_mph) < (
    SELECT AVG(trip_distance / TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND)) * 3600
    FROM taxi_data_updated -- Using the updated dataset
    WHERE trip_distance > 0
      AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) > 0
)
GROUP BY PULocationID
ORDER BY avg_speed_mph
LIMIT 5;


Data quality steps:

Filtering out invalid rows: Rows with zero distance or zero time taken are discarded, ensuring that only valid data is used for calculating the average speed.
# Filter out rows with zero distance or zero time taken
valid_data = taxi_data[(taxi_data['trip_distance'] > 0) & (taxi_data['time_taken'] > 0)]

  some data in the file isn't within the specified date range

To scale the solution to work with a 1 TB dataset with 10 GB of new data arriving each day, several changes would be necessary:

Data partitioning and indexing: Partitioning the data and creating appropriate indexes can improve the efficiency of data retrieval and processing, especially when dealing with large datasets.
cloud-based storage solutions.

Incremental processing for new data: Instead of processing the entire dataset each time, incremental processing can be used to process only the new data that arrives each day. 

distributed computing:
These frameworks can distribute the data across multiple nodes in a cluster and perform computations in parallel, allowing for efficient processing of large datasets.

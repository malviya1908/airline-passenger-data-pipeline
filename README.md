# Airlines Data Processing on GCP using Dataproc
## Project Overview
This project involves processing and transforming airlines flight booking data using **Apache Spark** on **Google Cloud Platform (GCP)**, specifically utilizing **Dataproc** for distributed data processing. The project focuses on cleaning and transforming the data, and then loading the cleaned data into **Google BigQuery** for further analysis. By leveraging the managed Dataproc cluster, the project benefits from the pre-installed dependencies and scalability of GCP's services.

## Table of Contents
1. Prerequisites
2. Technologies used
3. Project Architecture
4. Data Flow
5. Dependencies
6. Input Files
7. Output Files
8. Transformations Applied
9. Conclusion

## Prerequisites
Before you can run this project, you need to ensure you have the following :
- **Google Cloud Platform (GCP) Account :** You need an active GCP account for setting up Dataproc cluster and interacting with BigQuery.
- **Apache Spark :** The project uses Apache Spark for data processing. If you are not using Dataproc, you must install Spark on your local machine.
- **Python (v3.6+) :** Required for running the PySpark code.
- **Google Cloud SDK :** To interact with GCP services like BigQuery and Dataproc cluster.

# Technologies used
1. **Programming Language :** Python
2. **Big Data Framework :** Apache Spark (PySpark)
3. **Cloud Platform :** Google Cloud Platform (GCP)
4. **GCP Technologies :**
   1. **Google Cloud Storage (GCS) :** For storing input data and output files.
   2. **Dataproc :** Managed Spark and Hadoop service used for data processing.
   3. **BigQuery :** Fully managed data warehouse where transformed data is loaded for further analysis.

# Project Architecture
This project follows the **ETL (Extract, Transform, Load)** architecture:
1. **Extract :** Load the flight bookings and passengers data from a Google Cloud Storage bucket.
2. **Transform :** Clean and process the data (e.g., changing date formats, cleaning negative prices, filtering airlines).
3. **Load :** Load the transformed data into Google BigQuery.
The steps are orchestrated through a Spark job running on a Google Dataproc cluster which processes the data and then writes the results to BigQuery.

### Architecture Diagram :
![Project Architecture](https://github.com/malviya1908/airline-passenger-data-pipeline/blob/main/architecture%20Diagram/project1_architecture.drawio.png)

# Data Flow
1. **Extract :** Data is loaded from GCS buckets using the load_data_from_gcs function.
2. **Transform :** Various transformations are applied:
   - Null values are dropped from the dataframe (drop_null_records_from_airlines_df).
   - Date formats are standardized (clean_booking_dates).
   - Separate time and date columns are created for Travel_Date and Arrival_Date (create_time_and_date_columns).
   - Flight duration is calculated (calculate_flight_duration).
   - Airline names are cleaned using a UDF (clean_airline_names).
   - Negative prices are corrected (clean_price_column).
   - Data from the passengers and airlines dataframes are joined (join_df).
3. **Load :** The final dataframe is written to BigQuery (write_to_BigQuery).

**_GCS Bucket for extracting Data :_**
![GCS Bucket](https://github.com/malviya1908/airline-passenger-data-pipeline/blob/main/screenshots/gcs.png)

**_BigQuery table Output where the transformed data is loaded :_** 
![BigQuery Output](https://github.com/malviya1908/airline-passenger-data-pipeline/blob/main/screenshots/bq_output.png)

# Input Files
The project requires the following input files:
- **passengers_data.csv :** Contains details about passengers.
- **flight_bookings_dirty.csv :** Contains raw flight booking data that requires cleaning.
Both files should be uploaded to a GCS bucket for access by the Dataproc cluster.

# Output Files
The output is written to Google BigQuery. The final table will contain cleaned and transformed data with the following columns:
- Booking_ID
- Booking_Date
- Airline
- Source
- Destination
- Traveling_Date
- Take_Off_Time
- Arriving_Date
- Landing_Time
- Duration_in_hours
- Price
- Passenger_ID
  
The BigQuery table will be updated in the specified dataset after the transformation is completed.
# Transformations Applied
1. **Date Formatting :** The Booking_Date column is transformed to a uniform date format.
2. **Flight Duration Calculation :** The duration between Travel_Date and Arrival_Date is calculated and added as a new column.
3. **Airline Name Correction :** A mapping is applied to correct invalid airline names using a UDF.
4. **Price Cleaning :** Negative values in the Price column are converted to positive values.
5. **Time and Date Columns :** New columns for Take_Off_Time, Traveling_Date, Landing_time, and Arriving_Date are created.

# Conclusion
By utilizing the combination of **Google Cloud Platform** services like **Dataproc** and **BigQuery**, this project provides an optimized approach to large-scale data processing. The ability to process, clean, and transform data with **Apache Spark** in a cloud environment ensures high performance, scalability, and ease of integration. This solution not only simplifies data workflows but also enables quick insights, helping businesses make data-driven decisions effectively.

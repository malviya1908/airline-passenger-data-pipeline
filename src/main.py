# Import modules globally
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Function to create a SparkSession
def create_spark_session(app_name="AirlinesData") :
    try :
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Successfully created Spark session.")
        return spark
    except Exception as e :
        print(f"Error creating Spark Session : {str(e)}")
        
# Function to load CSV data from GCS bucket
def load_data_from_gcs(spark, file_path, header=True, inferschema=True, mode="PERMISSIVE"):
    try:
        df = spark.read.format("csv")\
                        .option("header", header)\
                        .option("inferschema", inferschema)\
                        .option("mode", mode)\
                        .load(file_path)
        print(f"Data successfully loaded from : {file_path}")
        return df
    except Exception as e :
        print(f"Error loading the file from : {file_path}")
        
# Function to drop null values from airlines dataframe
def drop_null_records_from_airlines_df(airlines_df) :
    try :
        drop_null_airlines_df = airlines_df.dropna(how="any")
        print("Successfully dropped Null values.")
        return drop_null_airlines_df
    except Exception as e :
        print(f"Error dropping the null values : {str(e)}")
        
# Function to change the date format of column Booking_Date
def clean_booking_dates(df) :
    try :
        flight_booking_date = df\
                                    .withColumn(
                                                "Booking_Date",
                                                when(col("Booking_Date").contains(" "), split(col("Booking_Date"), " ")[0])
                                                .otherwise(col("Booking_Date"))
                                    ).withColumn(
                                                "Booking_Date",
                                                coalesce(
                                                    to_date("Booking_Date", "yyyy-MM-dd"),
                                                    to_date("Booking_Date", "dd/MM/yyyy")
                                                )
                                    )
        print(f"Successfully changed the Date format for the Booking_Date column")
        return flight_booking_date
    except Exception as e :
        print(f"Error while changing the date format : {str(e)}")
        
# Function for Creating seperate time and date columns
def create_time_and_date_columns(df) :
    try :
        flight_travel_arrival_time_df = df\
                                            .withColumn("Take_Off_Time", date_format("Travel_Date", "HH:mm:ss"))\
                                            .withColumn("Traveling_Date", to_date("Travel_Date", "yyyy-MM-dd"))\
                                            .withColumn("Landing_time", date_format("Arrival_Date", "HH:mm:ss"))\
                                            .withColumn("Arriving_Date", to_date("Travel_Date", "yyyy-MM-dd"))
        print("Successfully created Seperate time and date columns")
        return flight_travel_arrival_time_df
    except Exception as e :
        print(f"Error while creating the seperate date and time columns : {str(e)}")
        
# Function to calculate the flight duration and then select relevant columns
def calculate_flight_duration(df) :
    try :
        flight_duration_df = df\
                                .withColumn(
                                        "Duration_in_hours",
                                        round((unix_timestamp("Arrival_Date") - unix_timestamp("Travel_Date"))/3600)
                                ).selectExpr(
                                        "Booking_ID", "Booking_Date", "Airline", "Source", "Destination",
                                        "Traveling_Date", "Take_Off_Time", "Arriving_Date", "Landing_Time",
                                        "Duration_in_hours", "Price", "Passenger_ID"
                                )
        print("Successfully created the Flight Duration column")
        return flight_duration_df
    except Exception as e :
        print(f"Error while creating Flight Duration column : {str(e)}")
        
# Function for Filtering out the airlines with names less than or equal to 3 characters
def filter_airline(df) :
    try :
        airline_names_df = df.filter(length(col("Airline")) > 3)
        print("Successfully filtered the data")
        return airline_names_df
    except Exception as e :
        print(f"Error while filtering the records : {str(e)}")
        
# Function to create a UDF for correcting the airline names 
def clean_airline_names() :
    try :
        # Defining a mapping for valid airline names
        airline_mapping = {
                "AirAsia ": "AirAsia India",
                "Spic": "SpiceJet",
                "Vistar": "Vistara",
                "Star Ai": "Star Air",
                "Akasa Ai": "Akasa Air",
                "Star A": "Star Air",
                "Go Fir": "Go First",
                "Akasa ": "Akasa Air",
                "AirAsia Indi": "AirAsia India"
            }
        def correct_name(name) :
            return airline_mapping.get(name,name)
        print("Successfully corrected the wrong airline names")
        return udf(correct_name, StringType())
    except Exception as e :
        print(f"Error while correcting the airline names : {str(e)}")
        
        
# Function to apply the UDF
def correct_names_in_dataframe(df, clean_name_udf) :
    return df.withColumn("Airline", clean_name_udf(col("Airline")))


# Function for correcting the negative values in price column
def clean_price_column(df) :
    try :
        price_df = df.withColumn("Price", abs(col("Price")))
        print("Successfully cleaned Price column")
        return price_df
    except Exception as e :
        print(f"Error while cleaning the price column : {str(e)}")
        
# Function to join the passengers_df and the final transformed price_df (airlines_df)
def join_df(df1,df2) :
    try :
        final_joined_df = df1.join(df2, ["Passenger_ID"], how="inner")
        print(f"Successfully joined both the dataframes")
        return final_joined_df
    except Exception as e :
        print(f"Error while joining the dataframes : {str(e)}")
        

# Function to write the final df to the BigQuery table 
def write_to_BigQuery(df, table_name, temp_bucket) :
    try :
        write_df = df.write \
                .format("bigquery") \
                .option("table", table_name) \
                .option("writeDisposition", "WRITE_APPEND") \
                .option("temporaryGcsBucket", temp_bucket) \
                .mode("append") \
                .save()
        print(f"Successfully loaded the data into the BigQuery table {table_name}")
        return write_df
    except Exception as e :
        print(f"Error while loading the dataframe : {str(e)}")
        
        
def main() :
    try :
        # Initialize Spark session
        spark = create_spark_session()
        
        # Load passengers and flight bookings data
        passengers_df = load_data_from_gcs(spark, "gs://dataproc_bucket_prac/airlines_data/passengers_data/passengers_dirty.csv")
        airlines_df = load_data_from_gcs(spark, "gs://dataproc_bucket_prac/airlines_data/flight_data/flight_bookings_dirty.csv")
        
        ######################################################## Data Cleaning and Transformation ########################################################
        # Drop null values from airlines dataframe
        airlines_drop_null_df = drop_null_records_from_airlines_df(airlines_df)
        
        # Changing the date format for the Booking_Date column
        flight_booking_date = clean_booking_dates(airlines_drop_null_df)
        
        # Creating seperate columns for time and date for Departure and Arrival
        flight_travel_arrival_time_df = create_time_and_date_columns(flight_booking_date)
        
        # Creating a duration column
        flight_duration_df = calculate_flight_duration(flight_travel_arrival_time_df)
        
        # filtering out airlines names with length less than 3 characters
        filter_airlines_name_df = filter_airline(flight_duration_df)
        
        # Create mapping for correcting the names of the airlines and also creating a udf
        clean_name_udf = clean_airline_names()
        
        # Applying the udf to apply the mapping for names
        correct_name_df = correct_names_in_dataframe(filter_airlines_name_df, clean_name_udf)
        
        # Cleaning the price Column
        price_df = clean_price_column(correct_name_df)
        
        # Joining the passengers df and finally trasnformed airline_df (price_df)
        final_joined_df = join_df(passengers_df, price_df)
        
        #################################################  Writing the final joined df to BigQuery  #############################################
        bq_table_name = "synthetic-nova-438808-k6.Airlines_dataset.airlines_data"
        bq_temp_bucket = "gs://dataproc_bucket_prac/airlines_data/bq_temp_folder"
        bq_write = write_to_BigQuery(final_joined_df, bq_table_name, bq_temp_bucket)     
        
    except Exception as e :
        print(e)
        
    finally:
        # Stop the Spark session to release resources
        if spark:
            spark.stop()
            print("Spark session stopped successfully.")
        

if __name__== "__main__":
    main()
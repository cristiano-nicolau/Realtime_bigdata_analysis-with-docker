from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import psycopg2

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaDataViewer").getOrCreate()

# Set log level and Spark configuration
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "venkat_stream") \
    .load()

# Cast the Kafka message to string
df1 = df.selectExpr("CAST(value AS STRING)")

# Clean and process the data
df3 = df1.withColumn("Body", regexp_replace("value", "\"", "")) \
    .withColumn("actual", split(col("Body"), ",")) \
    .withColumn("Timestamp", to_timestamp(col("actual").getItem(0), "yyyy-MM-dd HH:mm:ss").cast("Timestamp")) \
    .withColumn("Bank_Name", col("actual").getItem(1)) \
    .withColumn("Open", col("actual").getItem(2).cast("decimal(38, 0)")) \
    .withColumn("High", col("actual").getItem(3).cast("decimal(38, 0)")) \
    .withColumn("Low", col("actual").getItem(4).cast("decimal(38, 0)")) \
    .withColumn("Close", col("actual").getItem(5).cast("decimal(38, 0)")) \
    .withColumn("Volume", col("actual").getItem(6).cast("decimal(38, 0)"))

df4 = df3.select("Timestamp", "Bank_Name", "Open", "High", "Low", "Close", "Volume")

# Print schema to verify data
df4.printSchema()

# Class to handle writing data to PostgreSQL
class AggInsertTimeDB:
    def process(self, row):
        # Use asDict to extract row fields as a dictionary
        row_dict = row.asDict()

        # Extract fields from the row_dict
        StartTime = str(row_dict['StartTime'])  # Use the column name 'StartTime'
        EndTime = str(row_dict['EndTime'])  # Use the column name 'EndTime'
        Bank_Name = row_dict['Bank_Name']
        Open = row_dict['Avg_OpenPrice']
        High = row_dict['Avg_HighPrice']
        Low = row_dict['Avg_LowPrice']
        Close = row_dict['Avg_ClosePrice']
        Volume = row_dict['Avg_Volume']
        
        try:
            # PostgreSQL connection
            connection = psycopg2.connect(user="postgres",
                                          password="postgres",
                                          host="Timescale_DB",                                          
                                          port="5432",
                                          database="demo_streaming")
            cursor = connection.cursor()

            # Corrected SQL query with formatted strings
            sql_insert_query = """
            INSERT INTO venkat_demo (StartTime, EndTime, Bank_Name, Avg_OpenPrice, Avg_HighPrice, Avg_LowPrice, Avg_ClosePrice, Avg_Volume)
            VALUES ('%s', '%s', '%s', %s, %s, %s, %s, %s)
            """ % (StartTime, EndTime, Bank_Name, Open, High, Low, Close, Volume)

            print("\nsql_insert_query ", sql_insert_query)
            cursor.execute(sql_insert_query)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into venkat_demo table")

        except (Exception, psycopg2.Error) as error:
            print("Failed inserting record into table {}".format(error))
        finally:
            if connection:
                cursor.close()
                connection.close()


# Windowed aggregation: Group by time window and Bank_Name
dfWindowed = df4.groupBy(window(df4.Timestamp, "5 seconds", "2 seconds"), df4.Bank_Name) \
    .mean().orderBy('window') \
    .select(
        col("window.start").alias("StartTime"), 
        col("window.end").alias("EndTime"), 
        "Bank_Name", 
        col("avg(Open)").alias("Avg_OpenPrice"),
        col("avg(High)").alias("Avg_HighPrice"), 
        col("avg(Low)").alias("Avg_LowPrice"),
        col("avg(Close)").alias("Avg_ClosePrice"), 
        col("avg(Volume)").alias("Avg_Volume")
    )

# Print schema for windowed aggregation
print("dfWindowed schema")
dfWindowed.printSchema()

# Write the output to console for debugging
df4.writeStream.format("console").outputMode("append").option('truncate', 'false').start()

# Write windowed data to PostgreSQL
dfWindowed.writeStream.foreach(AggInsertTimeDB()).outputMode("complete").option('truncate', 'false').start()

# Write the windowed data to console
dfWindowed.writeStream.format("console").outputMode("complete").option('truncate', 'false').start().awaitTermination()

# Instructions for running Spark jobs
# ---> to run in yarn cluster
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --master yarn spark_submit.py

# ---> to run in spark standalone
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_submit.py

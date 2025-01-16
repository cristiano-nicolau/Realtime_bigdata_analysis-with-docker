# About the Project
The project aims to develop a real-time analysis system using Apache Kafka and Apache Spark. The system will collect real-time data and stream the data into Kafka. Apache Spark will then be used to process and analyze the data in real-time. The processed data will be visualized using appropriate visualizations and graphs.

The system will have two main components:

* **Data Collection:** This component will collect real-time data from various sources such as IOT, SCADA, CCTV, Stock Indexes, Weather Data ...etc. The data will be cleaned and transformed into a structured format before streaming it into Kafka.
      
* **Data Processing and Visualization:** This component will process the real-time data streams using Apache Spark. Spark will perform various real-time analysis tasks such as trend analysis, stock prediction, and outlier detection. The processed data will then be visualized using interactive dashboards and graphs to provide real-time insights into the stock market trends.

The system will be scalable, allowing it to handle a large volume of data streams and perform real-time analysis.

The project will be implemented using a combination of technologies such as **Apache Kafka, Apache Spark, Docker , Jupyter Notebook, and a front-end visualization tool such as Grafana**. The implementation will be based on a  On-premises architecture, ensuring high availability and scalability of the system.

## What is Docker?
In simple terms, Docker is a software platform that simplifies the process of building, running, managing and distributing applications. It does this by virtualizing the operating system of the computer on which it is installed and running.

## Why use Docker?
Using Docker lets you ship code faster, standardize application operations, seamlessly move code, and save money by improving resource utilization. With Docker, you get a single object that can reliably run anywhere. Docker's simple and straightforward syntax gives you full control. Wide adoption means there's a robust ecosystem of tools and off-the-shelf applications that are ready to use with Docker.

## Project Flow Chart
![flowchart](https://user-images.githubusercontent.com/90943529/217798777-82aae959-6260-4d0e-80a6-d8c674b77225.png)

## Updated Architecture

The architecture of the system has been updated. Now, Apache Spark is also running in Docker, with all components being deployed in containers. This containerized approach ensures consistency across different environments, simplifies the deployment process, and enhances the scalability and maintainability of the system.


## How to Run the Project

To run the project, follow these steps:

1. Clone the repository to your local machine.

2. Navigate to the project directory.

3. Run the following command to start the Docker containers:

```bash
docker-compose up
```

4. Open a web browser and navigate to the following URLs:

* **Kafka Manager:** [http://localhost:9000](http://localhost:9000)

You have to create the ```venkat_strea``` topic in the Kafka Manager.

5. Open a terminal and copy the ```spark_submit.py``` file to the ```spark-master``` container using the following command:

```bash
docker cp spark_submit.py spark-master:/spark_submit.py
```
You have to do this to submit the Spark job to the Spark cluster.

6. Open a terminal and go to the database container using the following command:

```bash
docker exec -it Timescale_DB bash
```

7. You have to garantee that the ```venkat_stream``` table is created in the database. If not, you have to create it using the following command:

```bash
Postgres Commands 

$ psql -U postgres
#psql (15.1 (Ubuntu 15.1-1.pgdg22.04+1))
#Type "help" for help.

postgres=# create database demo_streaming;
#CREATE DATABASE

postgres=# \c demo_streaming
#You are now connected to database "demo_streaming" as user "postgres".

demo_streaming=# create table venkat_demo (StartTime timestamptz not null, EndTime timestamptz not null, Bank_Name text, Avg_OpenPrice double precision, Avg_HighPrice double precision, Avg_LowPrice double precision, Avg_ClosePrice double precision, Avg_Volume double precision);

#CREATE TABLE


demo_streaming=# \d venkat_demo

                         Table "public.venkat_demo"
     Column     |           Type           | Collation | Nullable | Default 
----------------+--------------------------+-----------+----------+---------
 starttime      | timestamp with time zone |           | not null | 
 endtime        | timestamp with time zone |           | not null | 
 avg_openprice  | double precision         |           |          | 
 avg_highprice  | double precision         |           |          | 
 avg_lowprice   | double precision         |           |          | 
 avg_closeprice | double precision         |           |          | 
 avg_volume     | double precision         |           |          | 

demo_streaming=# select create_hypertable('venkat_demo', 'starttime');

demo_streaming=# \d venkat_demo
                         Table "public.venkat_demo"
     Column     |           Type           | Collation | Nullable | Default 
----------------+--------------------------+-----------+----------+---------
 starttime      | timestamp with time zone |           | not null | 
 endtime        | timestamp with time zone |           | not null | 
 avg_openprice  | double precision         |           |          | 
 avg_highprice  | double precision         |           |          | 
 avg_lowprice   | double precision         |           |          | 
 avg_closeprice | double precision         |           |          | 
 avg_volume     | double precision         |           |          | 
Indexes:
    "venkat_demo_starttime_idx" btree (starttime DESC)
Triggers:
    ts_insert_blocker BEFORE INSERT ON venkat_demo FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker()


```

8. Open a terminal and go to the ```spark-master``` container using the following command:

```bash
docker exec -it spark-master bash
```

9. Run the following command to submit the Spark job:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 spark_submit.py

```

10. Open a web browser and navigate to the following URLs:

* **Grafana:** [http://localhost:3000](http://localhost:3000)

11. Log in to Grafana using the following credentials:

* **Username:** admin
* **Password:** Venkat3039

12. Add a new data source in Grafana using the following details:

* **Name:** TimescaleDB
* **Type:** PostgreSQL
* **Host:** Timescale_DB
* **Database:** demo_streaming
* **User:** postgres
* **Password:** password

You can now create dashboards and visualizations in Grafana to monitor the real-time data streams.
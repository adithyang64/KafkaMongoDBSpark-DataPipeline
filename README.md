# KafkaMongoDBSpark-DataPipeline

This repo contains the codebase of a Data Pipeline of a HealthCare dataset. The DataPipeline invloves creation of a simulated Streaming data from a set of CSV Files to a Kafka Topic which in turn dumps the data to NoSQL MongoDB,further from which analysis of the data is performed using Spark in DataBricks Platform. 



## Project Architecture:

![Kafka-Mongo-Spark Architecture Diagram](https://github.com/adithyang64/KafkaMongoDBSpark-DataPipeline/assets/67658457/a5125bb8-12bb-4292-8997-1c43131dfc51)


## Data Description

The Dataset contains details about the Covid Infection spread across Korea, and it includes Data about Patient, Case, Region, Time Series Trend, etc.

Just for simplicity, we choose 3 CSV's as sample from the available dataset to be read and streamed to Kafka.

## Tech Stack
- Kafka
- MongoDB
- Python
- Spark
- DataBricks

## Setup 
Kakfa needs to be setup (Confluent Kafka is bwing used here) 
and Also steps needs to be followed to create Kafka Topic's for the data which gets streamed from those 3 CSV Files. And Proper Schema needs to be set for each topic as per the columns present in the csv file. 

Further, a Mongo_DB Cluster with a Database and Collections to which the streamed Data needs to be dumped is also to be created.

Once above steps is completed, execute the below files

Run the main.py in Terminal 1, which streams data from each csv file one after the other
```
main.py
```

Run the kafka_consumer_all.py which reads data from each created Kafka Topics and dumps into the respective MongoDB Collection.
```
kafka_consumer_all.py
```

Subsequently, a DataBricks Environment with Compute Clusters attached is setup and the analysis of the data is being done. MongoDB-Spark.ipynb contains the sample code which does basic analysis and trasformation of the stored data from MongoDB. 


## Scope of Improvement
This Pipeline can be improved to bring into picture the data from the other csv files and increase the analysis carried out using the same.
Also, the Codebase can be improved to dockerized into a container. 

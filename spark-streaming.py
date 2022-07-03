import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

# creating spark session
spark = SparkSession  \
.builder  \
.appName("Retail")  \
.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading Kafka Stream
lines = spark  \
.readStream  \
.format("kafka")  \
.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
.option("subscribe","real-time-project")  \
.option("startingOffsets", "earliest") \
.load()

lines.printSchema()


# Casting value as Stringwhich help to read column better
kafkaDF = lines.selectExpr("cast(value as string)")
kafkaDF.printSchema()

# creating user defined schema which help to read operate data better
schema = StructType() \
.add("invoice_no",StringType()) \
.add("country",StringType()) \
.add("timestamp",TimestampType()) \
.add("type",StringType()) \
.add("items", ArrayType(StructType() \
.add("SKU", StringType()) \
.add("title", StringType()) \
.add("unit_price", DoubleType()) \
.add("quantity", IntegerType())))

# creating inital DF as per user define schema
initialDF = kafkaDF.select(from_json(col("value"), schema).alias("data"))

initialDF.printSchema()

explodeInitialDF=initialDF.select(col("data.*"))

# final explosion of column as per items too
finalInputDF = explodeInitialDF.select(col("invoice_no"),col("country"),col("timestamp"),col("type"),explode("items").alias("new_items"))

finalDF=finalInputDF.select(col("invoice_no"),col("country"),col("timestamp"),col("type"),col("new_items.*"))

#Calcualting total cost by multiplying unit_price with quantity
customDF = finalDF.select(col("invoice_no"),col("country"),col("timestamp"),col("type"), col("SKU"), col("title"), col("unit_price"), col("quantity"), (col("unit_price")*col("quantity")).alias("total_cost"))

#Using UDF to create new columns of is_order & is_return
#UDF function for is_order
@udf(returnType=IntegerType())
def is_order(x):
    if x=='ORDER':
        return 1
    else:
        return 0

#applying UDF to create new DF
orderDF=customDF.select(col("*"), \
    is_order(col("type")).alias("is_order") )

#UDF for is_return column
@udf(returnType=IntegerType())
def is_return(x):
    if x=='ORDER':
        return 0
    else:
        return 1

returndf=orderDF.select(col("*"), \
    is_return(col("type")).alias("is_return") )

#dropping type column because dont need it as per assignment. Final Input DF
finalDFInput=returndf.drop(col("type"))

#checking Schema
finalDFInput.printSchema()

#Initial qrite stream query to write final input data on console
inputQuery = finalDFInput.groupBy("invoice_no","country","timestamp") \
       .agg(sum("total_cost").alias("total_cost"), \
            sum("quantity").alias("total_items")).writeStream  \
.outputMode("update")  \
.format("console")  \
.trigger(processingTime='1 minute') \
.start()

#Startig KPI Spark Query
#First KPI i.e. Time Based KPI
time_kpi_stream = finalDFInput \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute" , "1 minute").alias("time")) \
    .agg(sum("total_cost"),avg("total_cost"),count("invoice_no").alias("OPM"), avg("is_return")) \
    .select("time",
            "OPM",
            format_number("sum(total_cost)",2).alias("total_sale_volume"),
            format_number("avg(total_cost)",2).alias("average_transaction_size"),
            format_number("avg(is_return)",2).alias("rate_of_return"))

time_kpi_stream.printSchema()

#Second KPI i.e. Time and Country Base KPI
time_country_kpi_stream = finalDFInput \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute" , "1 minute").alias("time"),"country") \
    .agg(sum("total_cost"), count("invoice_no").alias("OPM"), avg("is_return")) \
    .select("time",
            "OPM",
            format_number("sum(total_cost)",2).alias("total_sale_volume"),
            format_number("avg(is_return)",2).alias("rate_of_return"))
            
time_country_kpi_stream.printSchema()


#Time KPI query to store output steam as json on 1 min window
timeKPIQuery = time_kpi_stream  \
.writeStream  \
.format("json") \
.outputMode("append")  \
.option("truncate", "false") \
.option("path", "time_kpi") \
.option("checkpointLocation","timecp1") \
.trigger(processingTime='1 minute') \
.start()


#Time and Country KPI query to store output steam as json on 1 min window
timeCountryQuery = time_country_kpi_stream  \
.writeStream  \
.format("json") \
.outputMode("append")  \
.option("truncate", "false") \
.option("path", "time_country_kpi") \
.option("checkpointLocation","timecountrycp") \
.trigger(processingTime='1 minute') \
.start()

timeCountryQuery.awaitTermination()



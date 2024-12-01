from pyspark.sql import SparkSession
from sys import stdin 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import col, to_date, count, date_format,desc

import sys
import os

spark = SparkSession.builder.appName("test").getOrCreate()

Schema = StructType([
    StructField("order_id1",IntegerType()),
    StructField("order_date1",DateType()),
    StructField("customer_id",IntegerType()),
    StructField("status",StringType())
])

df = spark.read.csv("file:///C:/Users/Lalan/PycharmProjects/Bigdata/inputfolder/orders.csv",schema = Schema,header=True)

filtered_df = df.filter(col("status")=="CLOSED")
formated_df = filtered_df.withColumn("order_date",to_date("order_date1","yyyy-MM-DD HH-mm-ss.S"))
format_df = formated_df.withColumn("month",date_format("order_date","yyyy-MM"))
selected_df = format_df.groupBy("month").agg(count("order_id1").alias("totla_closed_orders")).orderBy("month").show()







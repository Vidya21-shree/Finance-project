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

closed_orders = df.filter(col("status")=="CLOSED")
closed_orders_with_date = closed_orders.withColumn("order_date",to_date(col("order_date1"),"yyyy-MM-dd HH:mm:ss.S"))
closed_monthly = closed_orders_with_date.withColumn("month",date_format("order_date","yyyy-MM"))
total_df = closed_monthly.groupBy("month").agg(count("*").alias("total_sales_per_moth"))
ordered_df = total_df.orderBy("month")
ordered_df.show()







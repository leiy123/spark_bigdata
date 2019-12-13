##################
## author: Starly
##################

from __future__ import print_function

import sys
from datetime import datetime
import time

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import split, col
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: updateProduct.py <project_order_dir>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("blackFridayOrderStreaming")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    orderSchema = StructType()\
        .add("bought_timestamp", "string")\
        .add("uid", "string")\
        .add("pid", "string")\
        .add("p_count", "integer")\
        .add("sub", "integer")\
        .add("if_fullfill", "integer")

    productSchema = StructType()\
        .add("pid", "string")\
        .add("registration_timestamp", "string")\
        .add("product_name", "string")\
        .add("retail_price", "integer")\
        .add("discounted_price", "integer")\
        .add("stock", "integer")

    # Create DataFrame representing the stream of input lines from connection to hdfs
    lines = spark.readStream\
        .format("csv")\
        .option("header", "true")\
        .option("sep", ",").schema(productSchema)\
        .load("hdfs:///user/pc71776/data/product.csv")
    
    lines = lines.where(col("stock").isNotNull())

    order = spark.sparkContext.textFile("hdfs:///user/pc71776/output_project/%s/part-000*"%sys.argv[1])
    order = order.map(lambda x: x.strip('[&]').split(', ')).toDF()

    # timestamp = datetime.fromtimestamp(time.time()).strftime('%m/%d/%Y %H:%M:%S')

    # lines = lines.withColumn('event_timestamp',from_unixtime(unix_timestamp('bought_timestamp', 'MM/dd/yyyy HH:mm:ss')))     # event datetime
    # lines = lines.withColumn('event_unixtime', unix_timestamp('bought_timestamp', 'MM/dd/yyyy HH:mm:ss'))      # event unix time
    # lines = lines.withColumn('processing_time',unix_timestamp(lit(timestamp),'MM/dd/yyyy HH:mm:ss').cast("timestamp"))     # processing datetime

    # TODO: add header for order dataframe
    header = "bought_timestamp,uid,pid,p_count,sub,if_fullfill"

    orderSub = order.collect()
    for row in orderSub:
        if row._6 == "1":
            lines = lines.withColumn('stock', F.when(col("pid") == row._3.strip("'"), row._5).otherwise(F.col("stock")))
        elif row._6 == "0":
            pass

    query = lines.select("pid","registration_timestamp","product_name","retail_price","discounted_price","stock")\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .option("truncate", "false")\
        .start()
    #.option("numRows", "30")

    query.awaitTermination()
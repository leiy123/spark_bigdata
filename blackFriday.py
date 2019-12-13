##################
## author: Starly
##################

from __future__ import print_function

import sys
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F


# Verify user id in order
def uidVerify(uid):
    global userUid

    for userRow in userUid:
        if userRow.uid == uid:
            return 1
    return 0

def transformation(x):
    global productPid

    orderRow = x.split(",")
    bought_timestamp = orderRow[0]
    uid = orderRow[1]
    pid = orderRow[2]
    p_count = orderRow[3]

    if uidVerify(uid):
        for productRow in productPid:
            if str(pid) == str(productRow.pid):
                sub = productRow.stock - int(p_count)
                orderRow.append(sub)
                if sub >= 0:
                    if sub > 100:
                        orderRow.append(0)
                        return orderRow
                    orderRow.append(1)
                    return orderRow
                else:
                    orderRow.append(0)
                    return orderRow
        orderRow.append("")
        orderRow.append(0)
        return orderRow
                
    else:
        orderRow.append("")
        orderRow.append(0)
        return orderRow

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("blackFridayOrderStreaming")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    ssc = StreamingContext(spark.sparkContext, 20)

    productSchema = StructType()\
        .add("pid", "string")\
        .add("registration_timestamp", "string")\
        .add("product_name", "string")\
        .add("retail_price", "integer")\
        .add("discounted_price", "integer")\
        .add("stock", "integer")

    product = spark.read\
        .format("csv")\
        .option("header", "true")\
        .option("sep", ",").schema(productSchema)\
        .load("hdfs:///user/pc71776/data/product.csv")

    productPid = product.where(col("stock").isNotNull())\
        .select("pid","stock").collect()

    userSchema = StructType()\
        .add("uid", "string")\
        .add("name", "string")\
        .add("address", "string")\
        .add("postcode", "string")\
        .add("state", "string")

    user = spark.read\
        .format("csv")\
        .option("header", "true")\
        .option("sep", ",").schema(userSchema)\
        .load("hdfs:///user/pc71776/data/user.csv")

    userUid = user.where(col("uid").isNotNull())\
        .select("uid").collect()

    # Load streaming data
    lines = ssc.textFileStream("hdfs:///user/pc71776/input_project")

    header = "bought_timestamp,uid,pid,p_count"
    Order = lines.filter(lambda x: x != header).map(lambda x: transformation(x))

    Order.pprint()
    Order.saveAsTextFiles("hdfs:///user/pc71776/output_project/project_order")

    ssc.start()
    ssc.awaitTermination()
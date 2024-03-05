import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import time

def main():
    
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("python_udf") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    df_cat = df.withColumn("category_name", addCategoryName()(df["category"]))
    df_cat.show()
    df_cat.count()
    end_time = time.time()
    print("Execution Time for Python UDF: {:.2f} seconds".format(end_time - start_time))

def addCategoryName():
    return f.udf(lambda x: "food" if int(x) < 6 else "furniture", StringType())
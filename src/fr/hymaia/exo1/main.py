import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    print("Hello world!")

    spark = SparkSession.builder \
    .master("local[*]") \
    .appName("wordcount") \
    .getOrCreate()

    #df = spark.read.csv("src/resources/exo1/data.csv", header=True, inferSchema=True)
    df = spark.read.csv("src/resources/exo1/data.csv", header=True, inferSchema=True)

    result = wordcount(df, "text")
    result.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")



def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

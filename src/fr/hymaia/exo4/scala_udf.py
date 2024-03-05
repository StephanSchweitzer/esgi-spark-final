import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

spark = SparkSession.builder.config('spark.jars', 'src/resources/exo4/udf.jar').appName("exo4").master("local[3]").getOrCreate()

def main():
    start_time = time.time()

    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    df_agg = df.withColumn("category_name", addCategoryName(f.col("category")))
    #forcing the trasnformations to be processed THIS HAS TO BE RECONSIDERED
    df_agg.count()
    end_time = time.time()
    print("Execution Time for Scala UDF: {:.2f} seconds".format(end_time - start_time))

def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))
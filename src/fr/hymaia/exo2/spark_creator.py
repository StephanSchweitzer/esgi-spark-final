from pyspark.sql import SparkSession

spark = SparkSession.builder \
.master("local[*]") \
.appName("exo2") \
.getOrCreate()

def make_spark_df_from_csv(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def make_df_from_data(data):
    return spark.createDataFrame(data)

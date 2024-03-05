import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo4.scala_udf import addCategoryName
import pyspark.sql.functions as f

class SparkSessionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.config('spark.jars', 'src/resources/exo4/udf.jar').appName("exo4").master("local[3]").getOrCreate()

    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class AddCategoryNameTest(SparkSessionTest):
    def testBasicReadAndWrite(self):
        df = self.spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
        df_agg = df.withColumn("category_name", addCategoryName(f.col("category")))
        output_path = "./tests/fr/hymaia/exo4/benchmarks/benchmark_test_output"

        df_agg.write.mode("overwrite").parquet(output_path)
        
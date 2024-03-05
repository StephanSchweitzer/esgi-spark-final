import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo4.no_udf import addCategoryName
import pyspark.sql.functions as f

def addCategoryName(col):
    return f.when(f.col(col) < 6, "food").otherwise("furniture")

class SparkSessionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("unittest") \
            .master("local[3]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class AddCategoryNameTest(SparkSessionTest):
    def testBasicReadAndWrite(self):
        df = self.spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
        categories = addCategoryName("category")
        df_cat = df.withColumn("category_name", categories)
        output_path = "./tests/fr/hymaia/exo4/benchmarks/benchmark_test_output"

        df_cat.write.mode("overwrite").parquet(output_path)
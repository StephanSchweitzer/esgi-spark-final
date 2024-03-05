import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo4.no_udf import addCategoryName, addTotalPriceCategoryDay, addTotalPricePerCategoryLast30Days


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
        output_path = "./tests/fr/hymaia/exo4/benchmarks/benchmark_test_output"
        
        df.write.mode("overwrite").parquet(output_path)
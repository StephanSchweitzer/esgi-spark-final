import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.fr.hymaia.exo4.scala_udf import addCategoryName

class AddCategoryNameScalaUDFTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Assuming 'spark.jars' points to the correct path of your UDF JAR file
        cls.spark = SparkSession.builder \
            .config('spark.jars', 'src/resources/exo4/udf.jar') \
            .appName("AddCategoryNameScalaUDFTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_addCategoryNameScalaUDF(self):
        data = [("0", "21"), ("1", "1")]
        df = self.spark.createDataFrame(data, ["id", "category"])
        transformed_df = df.withColumn("category_name", addCategoryName(col("category")))
        self.assertIn("category_name", transformed_df.columns)
        
        results = transformed_df.select("id", "category_name").collect()
        expected_results = {"0": "furniture", "1": "food"}

        for row in results:
            self.assertEqual(expected_results[row["id"]], row["category_name"])

        expected_columns = ["id", "category", "category_name"]
        self.assertEqual(expected_columns, transformed_df.columns)

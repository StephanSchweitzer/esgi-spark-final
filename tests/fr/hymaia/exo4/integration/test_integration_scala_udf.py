import unittest
from src.fr.hymaia.exo4.scala_udf import addCategoryName
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class ScalaUdfIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ScalaUdfIntegrationTest") \
            .master("local[3]") \
            .config('spark.jars', 'src/resources/exo4/udf.jar') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_addCategoryNameScalaUDF(self):
        data = [("0", "1"), ("1", "7")]
        schema = ["id", "category"]
        df = self.spark.createDataFrame(data, schema=schema)
        df_with_category_name = df.withColumn("category_name", addCategoryName(f.col("category")))
        results = df_with_category_name.collect()
        expected_results = [
            {"id": "0", "category": "1", "category_name": "food"},
            {"id": "1", "category": "7", "category_name": "furniture"}
        ]

        for result in results:
            expected = next((item for item in expected_results if item["id"] == result["id"]), None)
            self.assertIsNotNone(expected)
            self.assertEqual(result["category_name"], expected["category_name"])

        self.assertIn("category_name", df_with_category_name.columns)

if __name__ == '__main__':
    unittest.main()

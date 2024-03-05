import unittest
from src.fr.hymaia.exo4.python_udf import addCategoryName
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class ScalaUdfIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("IntegrationTest") \
            .master("local[3]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_addCategoryNameScalaUDF(self):
        data = [("0", "22"), ("1", "1")]
        schema = ["id", "category"]
        df = self.spark.createDataFrame(data, schema=schema)
        df_with_category_name = df.withColumn("category_name", addCategoryName()(df["category"]))
        results = df_with_category_name.collect()
        expected_results = [
            {"id": "0", "category": "22", "category_name": "furniture"},
            {"id": "1", "category": "1", "category_name": "food"}
        ]

        for result in results:
            expected = next((item for item in expected_results if item["id"] == result["id"]), None)
            self.assertIsNotNone(expected)
            self.assertEqual(result["category_name"], expected["category_name"])

        expected_columns = ["id", "category", "category_name"]
        self.assertEqual(expected_columns, df_with_category_name.columns)

if __name__ == '__main__':
    unittest.main()

import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo4.python_udf import addCategoryName

class AddCategoryNameTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("AddCategoryNameTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_addCategoryName(self):
        data = [("0", "1"), ("1", "7")]
        df = self.spark.createDataFrame(data, ["id", "category"])
        transformed_df = df.withColumn("category_name", addCategoryName()(df["category"]))

        self.assertIn("category_name", transformed_df.columns)

        results = transformed_df.select("id", "category_name").collect()
        expected_results = {"0": "food", "1": "furniture"}

        for row in results:
            self.assertEqual(expected_results[row["id"]], row["category_name"])

        expected_columns = ["id", "category", "category_name"]
        self.assertEqual(expected_columns, transformed_df.columns)

if __name__ == '__main__':
    unittest.main()

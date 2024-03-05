import unittest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from src.fr.hymaia.exo4.no_udf import addCategoryName, addTotalPriceCategoryDay, addTotalPricePerCategoryLast30Days


class DataPipelineIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("IntegrationTest") \
            .master("local[3]") \
            .getOrCreate()

        # Prepare test data
        cls.data = [
            ("0", datetime.now().date(), "5", 10.0),  # Category that will be named 'food'
            ("1", datetime.now().date() - timedelta(days=1), "7", 20.0),  # Category that will be named 'furniture'
            ("2", datetime.now().date() - timedelta(days=29), "5", 15.0),  # Older 'food' to test 30 day window
            ("3", datetime.now().date() - timedelta(days=31), "5", 5.0)  # 'food' outside the 30 day window
        ]
        cls.schema = ["id", "date", "category", "price"]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pipeline(self):
        df = self.spark.createDataFrame(self.data, schema=self.schema)

        # Applying transformations
        df = df.withColumn("category_name", addCategoryName("category"))
        df = addTotalPriceCategoryDay(df, "date", "category_name")
        df = addTotalPricePerCategoryLast30Days(df, "date", "category_name", "price")

        # Collect and validate results
        results = df.collect()

        # Assertions to validate the integration logic
        # Here you would assert specific expectations based on your functions' logic
        # For example, ensuring that 'total_price_per_category_per_day_last_30_days' is correctly calculated

        # Asserting the existence of new columns
        self.assertIn("category_name", df.columns)
        self.assertIn("total_price_per_category_per_day", df.columns)
        self.assertIn("total_price_per_category_per_day_last_30_days", df.columns)

        # Specific logic assertions would be based on expected outcomes from your transformation logic
        # Example assertion (you would adjust according to your specific logic and expected outcomes):
        for row in results:
            if row['category_name'] == 'food':
                self.assertTrue(row['total_price_per_category_per_day_last_30_days'] <= 25.0)  # Example condition
            elif row['category_name'] == 'furniture':
                self.assertTrue(row['total_price_per_category_per_day_last_30_days'] <= 20.0)  # Example condition

if __name__ == '__main__':
    unittest.main()

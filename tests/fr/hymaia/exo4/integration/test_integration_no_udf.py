import unittest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from src.fr.hymaia.exo4.no_udf import addCategoryName, addTotalPriceCategoryDay, addTotalPricePerCategoryLast30Days


class NoUdfIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("IntegrationTest") \
            .master("local[3]") \
            .getOrCreate()

        cls.data = [
            ("0", datetime.now().date(), "5", 10.0),
            ("1", datetime.now().date() - timedelta(days=1), "7", 20.0),
            ("2", datetime.now().date() - timedelta(days=29), "5", 15.0),
            ("3", datetime.now().date() - timedelta(days=31), "5", 5.0)
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
        #df.show()
        results = df.collect()
        self.assertIn("category_name", df.columns)
        self.assertIn("total_price_per_category_per_day", df.columns)
        self.assertIn("total_price_per_category_per_day_last_30_days", df.columns)

        expected_results = \
        [("0", datetime.now().date(), "food", 10.0, 25.0),
        ("1", datetime.now().date() - timedelta(days=1), "furniture", 20.0, 20.0),
        ("2", datetime.now().date() - timedelta(days=29), "food", 15.0, 25.0),
        ("3", datetime.now().date() - timedelta(days=31), "food", 5.0, 25.0)]

        # for row in results:
        #     self.assertEqual(expected_results[row["category_name"]], row["total_price_per_category_per_day_last_30_days"])
        results_sorted = sorted(results, key=lambda x: x[0])  # Assuming 'id' can be used as a sort key
        expected_sorted = sorted(expected_results, key=lambda x: x[0])

        # Iterate through sorted results and expected outcomes for comparison
        for actual, expected in zip(results_sorted, expected_sorted):
            self.assertEqual(actual["id"], expected[0])
            self.assertEqual(actual["date"], expected[1])
            self.assertEqual(actual["category_name"], expected[2])
            self.assertEqual(actual["price"], expected[3])
            self.assertAlmostEqual(actual["total_price_per_category_per_day_last_30_days"], expected[4], places=2)

if __name__ == '__main__':
    unittest.main()

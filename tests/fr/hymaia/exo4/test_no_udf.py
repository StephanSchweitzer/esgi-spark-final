import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo4.no_udf import addCategoryName, addTotalPriceCategoryDay  # Adjust import path as needed

class TestNoUdf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("unittest") \
            .master("local[2]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_addCategoryName(self):
        data = [("0", "2019-02-16", "6", 40.0)]
        df = self.spark.createDataFrame(data, ["id", "date", "category", "price"])
        transformed_df = df.withColumn("category_name", addCategoryName("category"))
        self.assertIn("category_name", transformed_df.columns)
        result = transformed_df.collect()[0]["category_name"]
        self.assertEqual(result, "furniture")

    def test_addTotalPriceCategoryDay(self):
        # Sample input DataFrame
        data = [("0", "2019-02-16", "food", 20.0),
                ("1", "2019-02-16", "food", 40.0),
                ("2", "2019-02-17", "furniture", 60.0),
                ("3", "2019-02-17", "furniture", 80.0)]
        schema = ["id", "date", "category_name", "price"]
        
        df = self.spark.createDataFrame(data, schema=schema)
        transformed_df = addTotalPriceCategoryDay(df, "date", "category_name")
        
        expected_data = [("0", "2019-02-16", "food", 20.0, 30.0),
                        ("1", "2019-02-16", "food", 40.0, 30.0),
                        ("2", "2019-02-17", "furniture", 60.0, 70.0),
                        ("3", "2019-02-17", "furniture", 80.0, 70.0)]
        expected_df = self.spark.createDataFrame(expected_data, schema=schema + ["total_price_per_category_per_day"])
        self.assertTrue(expected_df.collect() == transformed_df.collect())

        expected_columns = ["id", "date", "category_name", "price", "total_price_per_category_per_day"]
        self.assertEqual(expected_columns, transformed_df.columns)

    def test_addTotalPricePerCategoryLast30Days(self):
        data = [("0", "2019-02-15", "food", 20.0),
                ("1", "2019-02-16", "food", 40.0),
                ("2", "2019-02-17", "furniture", 100.0)]
        schema = ["id", "date", "category_name", "price"]
        
        df = self.spark.createDataFrame(data, schema=schema)
        transformed_df = addTotalPricePerCategoryLast30Days(df, "date", "category_name")
        
        results = transformed_df.select("category_name", "total_price_per_category_per_day_last_30_days").distinct().collect()
        expected_results = {"food": 60.0, "furniture": 100.0}
        for row in results:
            self.assertEqual(expected_results[row["category_name"]], row["total_price_per_category_per_day_last_30_days"])

        expected_columns = ["id", "date", "category_name", "price", "total_price_per_category_per_day_last_30_days"]
        self.assertEqual(expected_columns, transformed_df.columns)
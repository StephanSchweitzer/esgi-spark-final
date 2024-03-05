import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
import time
from pyspark.sql.types import DateType

def main():
    start_time = time.time()

    spark = SparkSession.builder \
        .appName("no_udf") \
        .master("local[3]") \
        .getOrCreate()

    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    categories = addCategoryName("category")
    df_cat = df.withColumn("category_name", categories)
    df_price = addTotalPriceCategoryDay(df_cat, "date", "category")
    df_final = addTotalPricePerCategoryLast30Days(df_price, "date", "category")
    df_final.show()
    end_time = time.time()
    print("Execution Time for Non-UDF: {:.2f} seconds".format(end_time - start_time))

def addCategoryName(col):
    return f.when(f.col(col) < 6, "food").otherwise("furniture")

def addTotalPriceCategoryDay(df, col1, col2):
    window = Window.partitionBy(col1, col2)
    dfWithtotalPriceCategoryDay = df.withColumn("total_price_per_category_per_day", f.sum("price").over(window))
    return dfWithtotalPriceCategoryDay

def addUpdatedTotalPricePerCategory(df, date_col, category_col, price_col):
    total_sum_per_category = df.groupBy(category_col) \
                                .agg(f.sum(price_col).alias("total_sum"))
    
    df_with_total = df.join(total_sum_per_category, on=[category_col], how="left")

    return df_with_total

def addTotalPricePerCategoryLast30Days(df, date_col, category_col, price_col):
    beginning_columns = df.columns
    max_date = df.agg(f.max(date_col).alias("max_date")).collect()[0]["max_date"]
    start_date = max_date - f.expr("INTERVAL 30 DAYS")

    df_last_30_days = df.filter((f.col(date_col) <= max_date) & (f.col(date_col) > start_date))
    sums_last_30_days = df_last_30_days.groupBy(category_col).agg(f.sum(price_col).alias("total_price_per_category_per_day_last_30_days"))

    df_with_total_sum = df.join(sums_last_30_days, category_col, "left")
    final_columns = beginning_columns + ["total_price_per_category_per_day_last_30_days"]
    df_with_total_sum = df_with_total_sum.select(final_columns)

    return df_with_total_sum

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
    dfWithtotalPriceCategoryDay = df.withColumn("total_price_per_category_per_day", f.avg("price").over(window))
    return dfWithtotalPriceCategoryDay

def addUpdatedTotalPricePerCategory(df, date_col, category_col, price_col):
    total_sum_per_category = df.groupBy(category_col) \
                                .agg(f.sum(price_col).alias("total_sum"))
    
    df_with_total = df.join(total_sum_per_category, on=[category_col], how="left")

    return df_with_total

def addTotalPricePerCategoryLast30Days(df, date_col, category_col, price_col):
    max_date = df.agg(f.max(date_col).alias("max_date")).collect()[0]["max_date"]
    start_date = max_date - f.expr("INTERVAL 30 DAYS")

    df_last_30_days = df.filter((f.col(date_col) <= max_date) & (f.col(date_col) > start_date))
    sums_last_30_days = df_last_30_days.groupBy(category_col).agg(f.sum(price_col).alias("total_price_per_category_per_day_last_30_days"))

    df_with_total_sum = df.join(sums_last_30_days, category_col, "left")
    final_columns = ["id", "date", category_col, price_col, "total_price_per_category_per_day_last_30_days"]
    df_with_total_sum = df_with_total_sum.select(final_columns)

    return df_with_total_sum

# def addTotalPricePerCategoryLast30Days(df, date_col, category_col):
#     df = df.withColumn(date_col, f.col(date_col).cast(DateType()))

#     days = lambda i: i * 86400
#     windowSpec = Window \
#         .partitionBy(category_col) \
#         .orderBy(f.col(date_col).cast("timestamp").cast("long")) \
#         .rangeBetween(-days(30), 0)
    
#     df = df.withColumn("total_price_per_category_per_day_last_30_days", 
#                        f.sum("price").over(windowSpec))
#     return df

# def addTotalPricePerCategoryLast30Days(df, date_col, category_col):
#     # Ensure the date column is in the correct format
#     df = df.withColumn(date_col, f.col(date_col).cast(DateType()))
    
#     # Convert date to Julian date to simplify the calculation, if not already
#     df = df.withColumn("julian_date", f.datediff(f.col(date_col), f.lit("1970-01-01")))
    
#     # Define the window specification
#     windowSpec = Window \
#         .partitionBy(category_col) \
#         .orderBy("julian_date") \
#         .rangeBetween(-30, 0)
    
#     # Calculate the rolling sum over the last 30 days
#     df = df.withColumn("total_price_per_category_per_day_last_30_days", 
#                        f.sum("price").over(windowSpec))
    
#     return df.drop("julian_date")


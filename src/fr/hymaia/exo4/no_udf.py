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
    #df_price = addTotalPriceCategoryDay(df_cat, "date", "category")
    #df_final = addTotalPricePerCategoryLast30Days(df_price, "date", "category")
    #df_final.show()
    df_cat.count()
    end_time = time.time() #8.3 seconds
    print("Execution Time for Non-UDF: {:.2f} seconds".format(end_time - start_time))

def addCategoryName(col):
    return f.when(f.col(col) < 6, "food").otherwise("furniture")

def addTotalPriceCategoryDay(df, col1, col2):
    window = Window.partitionBy(col1, col2)
    dfWithtotalPriceCategoryDay = df.withColumn("total_price_per_category_per_day", f.avg("price").over(window))
    return dfWithtotalPriceCategoryDay

def addTotalPricePerCategoryLast30Days(df, date_col, category_col):
    df = df.withColumn(date_col, f.col(date_col).cast(DateType()))

    days = lambda i: i * 86400
    windowSpec = Window \
        .partitionBy(category_col) \
        .orderBy(f.col(date_col).cast("timestamp").cast("long")) \
        .rangeBetween(-days(30), 0)
    
    df = df.withColumn("total_price_per_category_per_day_last_30_days", 
                       f.sum("price").over(windowSpec))
    return df


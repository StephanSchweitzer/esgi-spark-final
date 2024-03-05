import pyspark.sql.functions as f

def filter_adults(df):
    return df.filter(df.age >= 18)

def join_clients_cities(clients_df, cities_df):
    return clients_df.join(cities_df, "zip")

def add_department_column(df_without_department):   
    joined_df_with_department = df_without_department.withColumn(
    "departement",
    f.when(
        df_without_department["zip"].startswith("20") & (df_without_department["zip"] <= "20190"), "2A"
    ).otherwise(
        f.when(
            df_without_department["zip"].startswith("20"), "2B"
        ).otherwise(
            df_without_department["zip"].substr(1, 2)
        )
    )
    )

    return joined_df_with_department

def write_csv_default(df_to_write, file_path):
    df_to_write.write.mode("overwrite").option("header", "true").csv(file_path)

def write_singular_csv(df_to_write, file_path_and_name):
    df_to_write.coalesce(1).write.csv(file_path_and_name, mode="overwrite", header=True)


def get_department_population(cities_df):
    return cities_df.groupBy("departement")\
                    .agg(f.count("*").alias("nb_people"))\
                    .orderBy(f.col("nb_people").desc(), f.col("departement"))

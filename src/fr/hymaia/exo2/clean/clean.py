import sys
from pathlib import Path

current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

import spark_creator as spark_create
import spark_functions as utilities

def main():
    zipcode_df = spark_create.make_spark_df_from_csv("src/resources/exo2/city_zipcode.csv")
    clients_df = spark_create.make_spark_df_from_csv("src/resources/exo2/clients_bdd.csv")

    adults_df = utilities.filter_adults(clients_df)
    joined_df = utilities.join_clients_cities(adults_df, zipcode_df)
    joined_df.write.mode("overwrite").parquet("data/exo2/output")

    joined_df = utilities.add_department_column(joined_df)
    utilities.write_csv_default(joined_df, "data/exo2/clean/")

main()
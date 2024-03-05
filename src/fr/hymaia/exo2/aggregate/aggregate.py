import sys
from pathlib import Path

current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))


import spark_creator as spark_create
import spark_functions as utilities

def main():
    cities_df = spark_create.make_spark_df_from_csv("data/exo2/clean/")
    department_population_df = utilities.get_department_population(cities_df)
    utilities.write_singular_csv(department_population_df, "data/exo2/aggregate/")

main()
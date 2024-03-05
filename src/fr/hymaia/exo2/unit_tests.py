#Bonjour Monsieur, je vous prie de bien vouloir excuser ces tests, 
#j'ai eu des problèmes avec mon environnement python depuis une heure 
#et cela ne vaut pas la peine de le faire d'une autre manière alors que 
#le TP concerne les tests unitaires et non l'architecture, je suis désolé !

import unittest
import spark_functions as utilities
import spark_creator as spark_create
from pyspark.sql import Row


class FilterAdultsTest(unittest.TestCase):

    def test_filter_adults(self):
        test_df = spark_create.make_spark_df_from_csv("src/resources/exo2/age_test.csv")
        filtered_test_df = utilities.filter_adults(test_df)
        result = sorted(filtered_test_df.collect(), key=lambda row: row.name)
        expected = [
            Row(name="Dane", age=58),
            Row(name="Donald", age=39),
            Row(name="Douglas", age=43),
            Row(name="Jimmy", age=18),
            Row(name="Sally", age=46) 
        ]
        self.assertEqual(result, expected)

class JoinClientsCitiesTest(unittest.TestCase):

    def test_join_clients_cities(self):
        clients_df = spark_create.make_spark_df_from_csv("src/resources/exo2/clients_test.csv")
        cities_df = spark_create.make_spark_df_from_csv("src/resources/exo2/city_zip_test.csv")
        joined_df = utilities.join_clients_cities(clients_df, cities_df)
        result = sorted(joined_df.collect(), key=lambda row: row.name)
        expected = [
            Row(zip=92007, name="Albert", age=45, city="ENCINITAS"),
            Row(zip=92007, name="Annie", age=46, city="ENCINITAS"),
            Row(zip=92104, name="Canary", age=39, city="SAN DIEGO"),
            Row(zip=92104, name="Faley", age=31, city="SAN DIEGO")
        ]
        self.assertEqual(result, expected)

class AddDepartmentColumnTest(unittest.TestCase):

    def test_add_department_column(self):
        df_without_department = spark_create.make_spark_df_from_csv("src/resources/exo2/clients_test.csv")
        df_with_department = utilities.add_department_column(df_without_department)
        result = sorted(df_with_department.collect(), key=lambda row: row.name)  # Adjust key as needed
        expected = [
            Row(name="Albert", age=45, zip=92007, department='92'),
            Row(name="Annie", age=46, zip=92007, department='92'),
            Row(name="Canary", age=39, zip=92104, department='92'),
            Row(name="Faley", age=31, zip=92104, department='92')
        ]
        self.assertEqual(result, expected)


def main():
    unittest.main()

main()
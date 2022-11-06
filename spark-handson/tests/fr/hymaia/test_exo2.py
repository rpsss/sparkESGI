from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.main import keep_adults, join_city_clients, add_zipcode
from src.fr.hymaia.exo2.clean.clean import clean
from src.fr.hymaia.exo2.aggregate.aggregate import aggregate
from pyspark.sql import Row


class TestKeepAdults(unittest.TestCase):
    def test_keep_adults(self):
        # Given
        input = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42),
            Row(name='Batman', age=17)
        ])

        # When
        actual = keep_adults(input)

        # Then
        expected = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42)
        ])

        self.assertListEqual(actual.collect(), expected.collect())


class TestBadKeepAdults(unittest.TestCase):
    def test_bad_keep_adults(self):
        # Given
        input = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42),
            Row(name='Batman', age=17),
            Row(name='Brutus Junior', age=18.5)
        ])

        # When
        actual = keep_adults(input)

        # Then
        expected = spark.createDataFrame([
            Row(name='Turing', age=21),
            Row(name='Hopper', age=42)
        ])

        self.assertListEqual(actual.collect(), expected.collect())


class TestJoinCityAndClients(unittest.TestCase):
    def test_join_city_clients(self):
        clients = spark.createDataFrame([
            Row(name='Turing', age=21, zip="10001"),
            Row(name='Bertrand', age=43, zip="91540"),
            Row(name='Rick', age=12, zip="91540"),
            Row(name='Bertrand', age=68, zip="75001")
        ])

        city = spark.createDataFrame([
            Row(zip='10001', city="VILLE"),
            Row(zip='91540', city="MENNECY"),
            Row(zip='75001', city="PARIS")
        ])

        actual = join_city_clients(clients, city, "zip")

        expected = spark.createDataFrame([
            Row(zip="10001", name='Turing', age=21, city="VILLE"),
            Row(zip="91540", name='Bertrand', age=43, city="MENNECY"),
            Row(zip="91540", name='Rick', age=12, city="MENNECY"),
            Row(zip="75001", name='Bertrand', age=68, city="PARIS")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())


class TestAddZipcode(unittest.TestCase):
    def test_add_zipcode(self):
        clients = spark.createDataFrame([
            Row(zip="10001", name='Turing', age=21, city="VILLE"),
            Row(zip="91540", name='Bertrand', age=43, city="MENNECY"),
            Row(zip="91540", name='Rick', age=12, city="MENNECY"),
            Row(zip="75001", name='Bertrand', age=68, city="PARIS"),
            Row(zip="20001", name='Jules', age=9, city="AJACCIO"),
        ])

        actual = add_zipcode(clients)

        expected = spark.createDataFrame([
            Row(zip="10001", name='Turing', age=21, city="VILLE", departement="10"),
            Row(zip="91540", name='Bertrand', age=43, city="MENNECY", departement="91"),
            Row(zip="91540", name='Rick', age=12, city="MENNECY", departement="91"),
            Row(zip="75001", name='Bertrand', age=68, city="PARIS", departement="75"),
            Row(zip="20001", name='Jules', age=9, city="AJACCIO", departement="2A")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())


class TestJobs(unittest.TestCase):
    def test_job1(self):
        clients = spark.createDataFrame([
            Row(name='Turing', age=21, zip="10001"),
            Row(name='Bertrand', age=43, zip="91540"),
            Row(name='Rick', age=12, zip="91540"),
            Row(name='Bertrand', age=68, zip="75001")
        ])

        city = spark.createDataFrame([
            Row(zip='10001', city="VILLE"),
            Row(zip='91540', city="MENNECY"),
            Row(zip='75001', city="PARIS")
        ])

        actual = clean(clients, city)

        expected = spark.createDataFrame([
            Row(zip="10001", name='Turing', age=21, city="VILLE", departement="10"),
            Row(zip="91540", name='Bertrand', age=43, city="MENNECY", departement="91"),
            Row(zip="75001", name='Bertrand', age=68, city="PARIS", departement="75")
        ])

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_jobs2(self):
        clean_df = spark.createDataFrame([
            Row(zip="10001", name='Turing', age=21, city="VILLE", departement="10"),
            Row(zip="91540", name='Bertrand', age=43, city="MENNECY", departement="91"),
            Row(zip="75001", name='Bertrand', age=68, city="PARIS", departement="75"),
            Row(zip="75002", name='Pablo', age=13, city="PARIS", departement="75"),
            Row(zip="91440", name='Gislène', age=43, city="ORMOY", departement="91")
        ])

        actual = aggregate(clean_df)

        expected = spark.createDataFrame([
            Row(departement="10", nb_people=1),
            Row(departement="91", nb_people=2),
            Row(departement="75", nb_people=2)
        ])

        self.assertCountEqual(actual.collect(), expected.collect())

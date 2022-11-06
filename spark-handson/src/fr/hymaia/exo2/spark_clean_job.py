import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.clean import clean
from src.fr.hymaia.exo2.aggregate.aggregate import aggregate


def spark_clean_job():
    spark = SparkSession.builder \
        .appName("exo2") \
        .master("local[*]") \
        .getOrCreate()

    df_city = spark.read.csv("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/src/resources/exo2/city_zipcode.csv",
                             header='true',
                             sep=",")

    df_clients = spark.read.csv("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/src/resources/exo2/clients_bdd.csv",
                                header='true',
                                sep=",")

    clean_df = clean(df_clients, df_city)

    clean_df.write.mode("overwrite").parquet("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/data/exo2/clean")

    df = spark.read.parquet("C:/Users/Raph/Desktop/esgiSPARKs/spark-handson/data/exo2/clean")

    aggregate_df = aggregate(df)

    aggregate_df \
        .write \
        .mode("overwrite") \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .save("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/data/exo2/aggregate")


if __name__ == '__main__':
    spark_clean_job()

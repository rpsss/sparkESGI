import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("WordCount")\
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/src/resources/exo1/data.csv",
                        header='true')

    df_count = wordcount(df, "text")

    df_count.write.partitionBy("count").mode("overwrite").parquet("C:/Users/Raph/Desktop/esgiSPARK/spark-handson/data/exo1/output2")


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

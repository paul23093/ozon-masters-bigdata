import os
import sys

SPARK_HOME = "/usr/hdp/current/spark2-client"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.7-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, concat, lit

conf = SparkConf()
conf.set("spark.jars.packages", "graphframes:graphframes:0.7.0-spark2.3-s_2.11")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def shortest_path(v_from, v_to, df, max_path_length=10):
    graph = df.distinct().cache()
    temp_df_c = graph.filter('c0 = {v_from}')
    i = 1
    output_columns = ['c0', lit(','), 'c1']
    while i < max_path_length:
        graph = graph.select(col(f'c{i-1}').alias(f'c{i}'), col(f'c{i}').alias(f'c{i+1}'))
        temp_df = temp_df_c
        temp_df_c.unpersist()
        temp_df_c = temp_df.join(graph, f'c{i}', 'inner').cache()
        temp_df_c.show()
        tmp = temp_df_c.filter(f'c{i+1} = {v_to}').count()
        output_columns.append(lit(','))
        output_columns.append(f'c{i+1}')
        if tmp > 0:
            break
        i += 1
    (
    temp_df_c
    .filter(f'c{i+1} = 34')
    .select(
        concat(*output_columns).alias('path')
    )
    .show(20, False)
    )
    temp_df_c.select("path").write.mode("overwrite").text("hw3_output")


if __name__ == '__main__':
    schema = StructType(fields=[
        StructField("c1", IntegerType()),
        StructField("c0", IntegerType())
    ])
    raw_graph = spark.read.csv(f"{sys.argv[3]}", schema=schema, sep='\t')
    shortest_path(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3])
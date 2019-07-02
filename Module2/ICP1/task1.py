import os

os.environ["SPARK_HOME"] = "/home/apandit7"
os.environ["HADOOP_HOME"]="/home/apandit7"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("WARN")
    lines = sc.textFile("sampletask1.txt", 1)

    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    counts.saveAsTextFile("output_task1")
    sc.stop()

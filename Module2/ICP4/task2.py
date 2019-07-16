import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
os.environ["SPARK_HOME"] = '/home/apandit7'
#conf =SparkConf().setAppName("Streaming word count")
sc = SparkContext("local[2]", "NetworkWordCount")
ssc= StreamingContext(sc, 15)
lines =ssc.socketTextStream("gw03.itversity.com", 19999)
words = lines.flatMap(lambda line:line.split(" "))
wordTuples =words.map(lambda word: (word, 1))
wordcount =wordTuples.reduceByKey(lambda x,y: x+y)
wordcount.pprint()
ssc.start()
ssc.awaitTermination()

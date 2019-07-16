import os
from pyspark.streaming import StreamingContext
from pyspark import *
os.environ["SPARK_HOME"] = '/home/apandit7'
sc = SparkContext("local[2]", "NetworkWordCount")
ssc=StreamingContext(sc,30)
lines = ssc.textFileStream('file:/home/apandit7/BigDataProgramming/ICP4/log/')
words=lines.flatMap(lambda line:line.split(' '))
wordtup=words.map(lambda word:(word,1))
wordcount=wordtup.reduceByKey(lambda x,y:x+y)
wordcount.pprint()
ssc.start()
ssc.awaitTermination()

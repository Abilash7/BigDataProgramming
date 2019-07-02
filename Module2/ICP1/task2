import os
os.environ["SPARK_HOME"] = "/home/apandit7"
os.environ["HADOOP_HOME"]="/home/apandit7"
from operator import add
from pyspark import SparkContext
#if _name_ == "_main_":
sc = SparkContext(appName="secCount")
lines = sc.textFile("sampletask2.txt", 1)
    #counts = lines.flatMap(lambda x: x.split(',')).map(lambda x: (x, 1)).reduceByKey(add)
    #output = counts.collect()
    #for (word, count) in output:
     #   print("%s, %i" % (word, count))
tokens = lines.flatMap(lambda x:x.split('\n'))
li=tokens.map(lambda x:x.split(','))
pairs=li.map(lambda x:(x[0]+'-'+x[1],x[3])).groupByKey().sortByKey()
p=pairs.partitionBy(numPartitions=2,)
q=[]
for x,y in pairs.collect():
        for m in y:
            q.append(m)
            j=sorted(q,reverse=True)
        print(x,j)

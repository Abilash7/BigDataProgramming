import os
from pyspark import *

os.environ["SPARK_HOME"] = '/Users/kranthikiranreddy/spark-2.2.0-bin-hadoop2.7'
from operator import add
from pyspark import SparkContext

keys = []


def friendmap(k):
    length=len(k)


    friends = k[1]
    user    = k[0]
    p=[]
    i = str(friends)

    for friend in friends:
            friends.remove(friend)
            p=friends[:]
            key1=sorted([user,friend])
            res=(",".join([str(i)for i in key1]))
            res2=(",".join([str(i)for i in p]))

            keys.append((res,res2))
            friends.append(friend)
    return keys

def mutual(v):
    v=list(v)
    k= ("".join([str(i) for i in v[1]]))
    Q=[]
    M=[]
    P=[]

    mut=k.split(',')
    for i in range(0,len(mut)):
        for j in range(1,len(mut)):
                if mut[i] not in P:
                 if (mut[i]==mut[j]) :

                  M.append(mut[i])
                  Q=M[:]
                  P.append(mut[i])
    return ((v[0],Q))

sc = SparkContext.getOrCreate()
Lines = sc.textFile("file:/Users/kranthikiranreddy/Downloads/facebook_combined.txt")
k=Lines.map(lambda x:(x.split(' ')[0],x.split(' ')[1]))
l=k.groupByKey()

L=[]
k=[]
print(l.take(2))
for x in l.take(30):
    for m in x[1]:
        L.append(m)
    k.append((x[0],L))
    L=[]
prdd=sc.parallelize(k)
qrdd=prdd.map(friendmap)
fqrdd=qrdd.flatMap(lambda x:(x))
rrdd=fqrdd.reduceByKey(lambda x,y:(x+','+y))
mutualfriends=rrdd.map(mutual)
print(mutualfriends.collect())

import os
os.environ["SPARK_HOME"] = '/home/apandit7'
os.environ["HADOOP_HOME"] = '/hadoop/apandit7'
from operator import add
def merge(left, right):
    result = []
    i, j = 0, 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    while (i < len(left)):
        result.append(left[i])
        i += 1
    while (j < len(right)):
        result.append(right[j])
        j += 1
    print('merge: ' + str(left) + '&' + str(right) + ' to ' + str(result))
    return result
def merge_sort(L):
    print('merge sort: ' + str(L))
    if len(L) < 2:
        return L[:]
    else:
        middle = len(L) // 2
        left = merge_sort(L[:middle])
        right = merge_sort(L[middle:])
        return merge(left, right)
from pyspark import SparkContext
from pyspark import *
#if _name_ == "_main_":
sc = SparkContext(appName="secCount")
a = sc.parallelize([[3, 5, 2, 4, 1]])
#b = a.map(lambda x: (x,1))
#print(b.collect())
#c = b.sortByKey()
#print(c.keys().collect())
z=a.map(merge_sort)
print(z.collect())

import os
#from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import SQLContext,Row
os.environ["SPARK_HOME"] = '/home/apandit7'
sc = SparkContext(appName="dataframe")
sq=SQLContext(sc)
#ds = sq.read.format("csv").option("header", "true").load("Survey.csv")
#df = sq.read.format('csv').options(header='true',encoding='UTF-8', inferschema='true').load("Survey.csv",header=True)
ds = sq.read.format("csv").option("header", "true").load("Survey.csv")
dsdup= ds.distinct()
ds.registerTempTable('ds')
dsdup.registerTempTable('dsdup')
minu= sq.sql("SELECT * FROM ds MINUS SELECT * FROM dsdup")
minu.show(5)
ds2=ds.limit(10)
undf1=ds.union(ds2)
undf1.show(5)
undf1.registerTempTable('undf1')
sqund=sq.sql("select * from undf1 order by Country")
sqund.show(5)
ds.printSchema()
undf2=sq.sql("SELECT * FROM ds LEFT JOIN dsdup on ds.country=dsdup.country")
undf3=sq.sql("SELECT * FROM ds JOIN dsdup on ds.treatment=dsdup.treatment")
undf2.show(5)
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
ds.registerTempTable("survey")
c=sq.sql("SELECT Age from survey")
c.show(5)
d=sq.sql("SELECT treatment,count(*) FROM survey group by treatment")
d.show()
dl=sq.sql("SELECT SUM(Age) FROM survey ")
dl.show()
dl2=sq.sql("SELECT AVG(Age) FROM survey ")
dl2.show()
dep1=Row(name='computerscience',id=str(1))
dep2=Row(name='mechanical',id='2')
employee=Row('name','employeename','id')
emp1=employee('computerscience','kranthi','1')
emp2=employee('mechanical','abhi','2')
depe1=Row(department=dep1,employee=[emp1])
depe2=Row(department=dep2,employee=[emp2])
print(depe1.department.name)
depedf1=[depe1]
depedf2=[depe2]
depdf1=sq.createDataFrame([depe1])
depdf2=sq.createDataFrame([depe2])
rd1=sq.read.json('unionoutpractice1')
rd1.show()
#ds.show(5)
#ds.printSchema()

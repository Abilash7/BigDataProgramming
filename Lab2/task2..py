import os
from pyspark import *
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField,LongType,StringType,TimestampType,DateType
os.environ["SPARK_HOME"] = '/home/apandit7'
sc = SparkContext(appName="dataframe")
sq=SQLContext(sc)
#matchesdf =sq.read.format("csv").option("header", "true").load("WorldCupMatches.csv")
#matchesdf.show(truncate=False)
#matchesdf.printSchema()
mySchema =StructType([
        StructField("Year",LongType(), True),
StructField("DateTime",StringType(), True),
StructField("Stage",StringType(), True),
StructField("Stadium",StringType(),True),
StructField("City",StringType(), True),
StructField("Home Team Name",StringType(), True),
StructField("Home Team Goals",IntegerType(), True),
StructField("Away Team Goals",IntegerType(), True),
StructField("Away Team Name",StringType(), True),
StructField("Win conditions",StringType(), True),
StructField("Attendance",IntegerType(), True),
StructField("Half-time Home Goals",IntegerType(), True),
StructField("Half-time Away Goals",IntegerType(), True),
StructField("Referee",StringType(), True),
StructField("Assistant1",StringType(), True),
StructField("Assistant2",StringType(), True),
StructField("RoundID",IntegerType(), True),
StructField("MatchID",LongType(), True),
StructField("Home Team Initials",StringType(), True),
StructField("Away Team Initials",StringType(), True)
        ])
        Structdf=sq.read.format("csv").schema(mySchema).option("header","true").load("WorldCupMatches.csv")
Structdf.show(10)
Structdf.printSchema()
#Structdf.show(10)
Structdf.registerTempTable('Structdf')
df= sq.sql("SELECT * FROM Structdf")
df.show(5)
#df.dropDuplicates(df.columns).show()
#df.show(5)
df1=df.limit(30)
df2=df.limit(50)
Uniondf=df1.unionAll(df2)
Uniondf.show()
Uniondf.registerTempTable('Uniondf')
#Uniondf.printSchema()
#dfquery3=sq.sql("SELECT Year,DateTime,Stage,Stadium from Structdf")
#dfquery3.show(10)
dfquery3=sq.sql("SELECT Year,City,`Home Team Goals` as Home_Team_Goals  from Structdf where City like '%Zurich%'")
dfquery3.show(50)
dfquery4=sq.sql("select City,Attendance from Structdf where Attendance>9000 order by Attendance desc limit 20")
#,Referee,RoundID,MatchID,`Home Team Initials` as Home_Team_Initials from Structdf where Attendance>4444  group by City")
dfquery4.show()
dfquery5=sq.sql("select * from Structdf left join Uniondf  on Structdf.City=Uniondf.City")
dfquery5.show()
dfquery6=Structdf.filter(Structdf['Half-time Home Goals']>2).show(10)
dfquery7=Structdf.groupBy("Home Team Goals").count().show(30)
dfquery8=sq.sql("select * from Structdf where `Home Team Name` like '%Argentina%' and Year like '%1930%'")
dfquery8.show()
dfquery9=Structdf.drop("Home Team Name", "City")
dfquery9.show(10)
#dfquery10=Structdf.select("Stage", "Attendance").write.save("StageAndAttendance.parquet")
dfquery10=dfquery9.withColumnRenamed("Home Team Goals", "Home_Goals").withColumnRenamed("Away Team Goals", "Away_Goals").withColumnRenamed("Away Team Name", "Away_Name").withColumnRenamed("Win co
nditions", "Wc").withColumnRenamed("Half-time Home Goals", "HalfHome").withColumnRenamed("Half-time Away Goals","HalfAway").withColumnRenamed("Home Team Initials","HomeIn").withColumnRenamed("Awa
y Team Initials","AwayIn")
dfquery11=dfquery10.write.parquet("Attendance_part1",mode="overwrite",partitionBy="Year",compression='gzip')
#dfquery10.rdd.take(2)

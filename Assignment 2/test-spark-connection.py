from pyspark.sql import SparkSession, SQLContext
import time

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.19.jar") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "postgresdb") \
    .option("user", "postgres") \
    .option("password", "test123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

sqlContext = SQLContext(spark)
df.createOrReplaceTempView("postgresdb")
startbackendtime=time.time()
    #spark.read.jdbc
start1aBE=time.time()
jdbcDF1aBE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select event,date(timestamp),count(*) from postgresdb group by event,date(timestamp) having event='opened' order by date(timestamp)) as X")
jdbcDF1aBE.show()
end1aBE=time.time()
print('Time for 1a Backend',end1aBE - start1aBE)

start1bBE=time.time()
jdbcDF1bBE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select event,date(timestamp),count(*) from postgresdb group by event,date(timestamp) having event='discussed' order by date(timestamp)) as X")
jdbcDF1bBE.show()
end1bBE=time.time()
print('Time for 1b Backend',end1bBE - start1bBE)

start2BE=time.time()
jdbcDF2BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select a.name,b.monthnumber,b.countscore \
from(select hello.name, max(events) even, hello.monthnumber \
from (select author as name,EXTRACT(MONTH from timestamp) monthnumber, count(event) events \
from postgresdb where event='discussed' \
group by EXTRACT(MONTH from timestamp), author) as hello \
group by hello.monthnumber,hello.name \
order by monthnumber,even desc)as a, \
(select hello.monthnumber, max(events) countscore \
from (select EXTRACT(MONTH from timestamp) monthnumber, count(event) events \
from postgresdb where event='discussed' \
group by EXTRACT(MONTH from timestamp), author) as hello \
group by hello.monthnumber)as b \
where a.monthnumber=b.monthnumber and a.even=b.countscore) as X")
jdbcDF2BE.show()
end2BE=time.time()
print('Time for 2 Backend',end2BE - start2BE)

start3BE=time.time()
jdbcDF3BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select a.name,b.week,b.countscore from(select hello.name, max(events) even, hello.week from (select author as name,date_trunc('week',timestamp) week, count(event) events \
from postgresdb where event='discussed' \
group by date_trunc('week',timestamp), author) as hello \
group by hello.week,hello.name \
order by week,even desc)as a, \
(select hello.week, max(events) countscore \
from (select date_trunc('week',timestamp) week, count(event) events \
from postgresdb where event='discussed' \
group by date_trunc('week',timestamp), author) as hello \
group by hello.week)as b \
where a.week=b.week and a.even=b.countscore) as X")
jdbcDF3BE.show()
end3BE=time.time()
print('Time for 3 Backend',end3BE - start3BE)


start4BE=time.time()
jdbcDF4BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select date_trunc('week',timestamp) weekstamp, count(pull_requestid) \
from postgresdb \
where event='opened' \
group by event,weekstamp \
order by weekstamp) as X")
jdbcDF4BE.show()
end4BE=time.time()
print('Time for 4 Backend',end4BE - start4BE)

start5BE=time.time()
jdbcDF5BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select EXTRACT(MONTH FROM timestamp) mon, count(event) \
from postgresdb \
where event='merged' and extract(year from timestamp)=2010 \
group by mon) as X")
jdbcDF5BE.show()
end5BE=time.time()
print('Time for 5 Backend',end5BE - start5BE)

start6BE=time.time()
jdbcDF6BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select date(timestamp), count(pull_requestId) \
from postgresdb \
group by date(timestamp) \
order by date(timestamp)) as X")
jdbcDF6BE.show()
end6BE=time.time()
print('Time for 6 Backend',end6BE - start6BE)

start7BE=time.time()
jdbcDF7BE = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/postgres", \
          properties={"user": "postgres", "password": "test123", "driver" : "org.postgresql.Driver"}, \
          table="(select author, count(event) events \
from postgresdb \
where event='opened' and extract(year from timestamp)=2011 \
group by author \
order by events desc \
limit 1) as X")
jdbcDF7BE.show()
end7BE=time.time()
print('Time for 7 Backend',end7BE - start7BE)

backendtime=time.time()

print('Total Backend time',backendtime-startbackendtime)

# df = sqlContext.sql("select * from pullreq")

finstart=time.time()

# #1a Ok
print('Ans Frontend 1a')
start1a=time.time()
df = spark.sql("select event,date(timestamp),count(*) from postgresdb group by event,date(timestamp) having event='opened' order by date(timestamp)")
df.show()
end1a=time.time()
print('Frontend 1a time',end1a-start1a)

# #1b Ok
print('Ans Frontend 1b')
start1b=time.time()
df = spark.sql("select event,date(timestamp),count(*) from postgresdb group by event,date(timestamp) having event='discussed' order by date(timestamp)")
df.show()
end1b=time.time()
print('Frotend 1b time',end1b-start1b)

#2
print('Ans Frontend 2')
start2=time.time()
df = spark.sql("select a.name,b.monthnumber,b.countscore \
from(select hello.name, max(events) even, hello.monthnumber \
from (select author as name,EXTRACT(MONTH from timestamp) monthnumber, count(event) events \
from postgresdb where event='discussed' \
group by EXTRACT(MONTH from timestamp), author) as hello \
group by hello.monthnumber,hello.name \
order by monthnumber,even desc)as a, \
(select hello.monthnumber, max(events) countscore \
from (select EXTRACT(MONTH from timestamp) monthnumber, count(event) events \
from postgresdb where event='discussed' \
group by EXTRACT(MONTH from timestamp), author) as hello \
group by hello.monthnumber)as b \
where a.monthnumber=b.monthnumber and a.even=b.countscore")
df.show()
end2=time.time()
print('Frotend 2 time',end2-start2)

#3 OK
print('Ans Frontend 3')
start3=time.time()
df = spark.sql("select a.name,b.week,b.countscore from(select hello.name, max(events) even, hello.week from (select author as name,date_trunc('week',timestamp) week, count(event) events \
from postgresdb where event='discussed' \
group by date_trunc('week',timestamp), author) as hello \
group by hello.week,hello.name \
order by week,even desc)as a, \
(select hello.week, max(events) countscore \
from (select date_trunc('week',timestamp) week, count(event) events \
from postgresdb where event='discussed' \
group by date_trunc('week',timestamp), author) as hello \
group by hello.week)as b \
where a.week=b.week and a.even=b.countscore")
df.show()
end3=time.time()
print('Frotend 3 time',end3-start3)

#4 ok
print('Ans Frontend 4')
start4=time.time()
df = spark.sql("select date_trunc('week',timestamp) weekstamp, count(pull_requestid) \
from postgresdb \
where event='opened' \
group by event,weekstamp \
order by weekstamp")
df.show()
end4=time.time()
print('Frotend 4 time',end4-start4)
#5
print('Ans 5')
start5=time.time()
df = spark.sql("select EXTRACT(MONTH FROM timestamp) mon, count(event) \
from postgresdb \
where event='merged' and extract(year from timestamp)=2010 \
group by mon")
df.show()
end5=time.time()
print('Frotend 5 time',end5-start5)

#6
print('Ans Frontend 6')
start6=time.time()
df = spark.sql("select date(timestamp), count(pull_requestId) \
from postgresdb \
group by date(timestamp) \
order by date(timestamp)")
df.show()
end6=time.time()
print('Frotend 6 time',end6-start6)

#7
print('Ans Frontend 7')
start7=time.time()
df = spark.sql("select author, count(event) events \
from postgresdb \
where event='opened' and extract(year from timestamp)=2011 \
group by author \
order by events desc \
limit 1")
df.show()
end7=time.time()
print('Frotend 7 time',end7-start7)

finend=time.time() #show time

print('Total time for frontend',finend-finstart)


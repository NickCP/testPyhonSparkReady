from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import *

conf = SparkConf().set('spark.driver.host','127.0.0.1')
sc = SparkContext("local", "App Name",conf = conf)
sql = SQLContext(sc)

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = sql.createDataFrame(departmentsWithEmployeesSeq1)

df1.show(4)
departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = sql.createDataFrame(departmentsWithEmployeesSeq2)

df2.show(4, truncate= True)

# my own dataframes
df3 = sql.createDataFrame([("48","Nick    ",), ("20","Roman     ",), ("18","Marta  ",), ("23","Nastya  ",), ("34"," ",)], ["age", "Name"])
df5 =sql.createDataFrame([("48","Nick    ",), ("20","Roman     ",), ("19","Marta  ",), ("25","Nastya  ",), ("34","Petrenko",)], ["age", "Name"])

# sorting dataframes with adding new column
df3.orderBy("age", ascending=False).show()
df3.withColumn("New name",trim(col("Name"))).show()
print("- - - - -- - -  -- - - - -- - ")
df3.withColumn("new name", regexp_replace(col("Name"), "Nick", "Kolya")).show()
# change value 48 on 20 in column AGE
print("- - - - -- - -  -- - - - -- - ")
df3.withColumn("new age", regexp_replace(col("Age"), "48", "20")).show()
print("- - - - -- - -  -- - - - -- - ")
# load data from txt file
df4 = sc.textFile("C://Users/mchub/Desktop/fie.txt")
csvDf =sql.read.csv("C://Users/mchub/Desktop/file.csv", header=True, inferSchema=True)
listt = df4.collect()
for i in listt:
    print(i)
print("------------")

# comparison two datasets
print("Dataset 5:")
df5.show()
print("Dataset 3:")
df3.show()
print("Exceptions:")
df3.exceptAll(df5).show()


# I join two tables
ta = df3.alias("ta")
tb = df5.alias("tb")
inner_join = ta.join(tb, ta.age == tb.age)
inner_join.show()
print("__________LEFT JOIN_________")
left_join = ta.join(tb, ta.age == tb.age, how='left')
left_join.show()
print("____________WITH FILTER__________#1")
left_join.filter(col('tb.name').isNull()).show()
print("____________WITH FILTER__________#2")
left_join.filter(col('tb.name').isNotNull()).show()

print("_______LOWER_______")
df3.withColumn("lower", lower(col("Name"))).show()

# Bad idea, it doesn`t work :(
print("______ TEST CSVDF_________")
csvDf.show()
csvDf.printSchema()


# withColumn - add new column with changes
#
# orderBy("age", ascending = False) - sort
#

# !!!!!!!!!! LOOK LIKE THIS!!!!!!!!!!!
#______ TEST CSVDF_________
#+-------------------+
#|First;Second;Third;|
#+-------------------+
#|             1;1;2;|
#|             2;2;3;|
#|             2;3;1;|
#+-------------------+
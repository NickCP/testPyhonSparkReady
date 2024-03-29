# In this code i demonstrate my skills
# 5 days
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
import random, string
from math import ceil
import pandas as pd
from pyspark.sql.functions import lit

# Configuration
conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext("local", "App Name", conf=conf)
sql = SQLContext(sc)


# my first task
data_period = sql.createDataFrame(
    [("1", "2019-01-01", "2019-12-31"), ("2", "2019-03-01", "2019-03-31"), ("3", "2019-01-01", "2019-03-31"),
     ("4", "2015-01-01", "2019-08-31"), ("5", "2018-01-01", "2019-12-31"), ("6", "2019-01-01", "2019-12-31"),
     ("7", "2020-01-01", "2020-03-31"), ("8", "2020-01-01", "2020-03-31"), ("9", "2020-01-01", "2020-03-31"),
     ("10", "2019-01-01", "2019-12-31"), ], ["trx", "earned_start_date", "earned_end_date"])

z = data_period.head(2)
print("ZZZZZ ",z[0][1])


data_period.show()
# the list if values from earned_start_date
list_of_earned_start_date = list(data_period.select("earned_start_date").collect())


min_earned_start_date = data_period.agg({"earned_start_date": "min"}).collect()[0][0]
max_earned_start_date = data_period.agg({"earned_start_date": "max"}).collect()[0][0]
max_earned_start_date_row = data_period.agg({"earned_start_date": "max"}).collect()[0]
min_earned_end_date = data_period.agg({"earned_end_date": "min"}).collect()[0][0]
max_earned_end_date = data_period.agg({"earned_END_date": "max"}).collect()[0][0]

# the list if values from earned_start_date
list_of_earned_start_date = list(data_period.select("earned_start_date").collect())
list_of_earned_end_date = list(data_period.select("earned_end_date").collect())

len_of_list_start = len(list_of_earned_start_date)
len_of_list_end = len(list_of_earned_end_date)

value_start = list_of_earned_start_date
value_end = list_of_earned_end_date

list_of_earned_end_date_filter = []
list_of_earned_start_date_filter = []
print("__________________________")


if max_earned_start_date > min_earned_end_date:
    for i in range(0, len_of_list_start):
        if value_start[i][0] != max_earned_start_date:
            list_of_earned_start_date_filter.append(value_start[i][0])
            list_of_earned_end_date_filter.append(value_end[i][0])

list_of_earned_start_date_filter.sort(reverse=True)
new_max_value_start = list_of_earned_start_date_filter[0]
list_of_earned_end_date_filter.sort(reverse=True)
new_max_value_end = list_of_earned_end_date_filter[0]
max_period = min_earned_start_date + " - " + new_max_value_end
exception_max_peroid = max_earned_start_date + " - " + max_earned_end_date
withincolumn_list = []
for i in range(0, len_of_list_start):
    if value_end[i][0] < max_earned_start_date:
        withincolumn_list.append(max_period)
    else:
        withincolumn_list.append(exception_max_peroid)
temporary_data = sql.createDataFrame([(l,) for l in withincolumn_list], ['max_period'])
data_period = data_period.withColumn("idx", monotonically_increasing_id())
temporary_data = temporary_data.withColumn("idx", monotonically_increasing_id())

final_data_period = data_period.join(temporary_data, data_period.idx == temporary_data.idx).drop("idx").orderBy("trx", ascending=True)
final_data_period.show()


print("min_earned_START_date:", min_earned_start_date)
print("max_earned_START_date:", max_earned_start_date)
print("min_earned_END_date:", min_earned_end_date)
print("max_earned_END_date:", max_earned_end_date)


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

# Create DataFrame with departments and employees
departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = sql.createDataFrame(departmentsWithEmployeesSeq1)

df1.show(4)
departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = sql.createDataFrame(departmentsWithEmployeesSeq2)
df2.show(4, truncate=True)

# Create two DataFrames for demonstration sorting, trim, join etc.
df3 = sql.createDataFrame(
    [("48", "Nick    ",), ("48", "Roman     ",), ("18", "Marta  ",), ("23", "Nastya  ",), ("34", None,)],
    ["age", "Name"])
df5 = sql.createDataFrame(
    [("48", "Nick    ",), ("20", "Roman     ",), ("19", "Marta  ",), ("25", "Nastya  ",), ("34", "Petrenko",)],
    ["age", "Name"])

# sorting dataframes raise=false
print("___SORTED ASCENDING=FALSE_______")
df3.orderBy("age", ascending=False).show()
print("___FILL EMPTY VALUES___")
df3.na.fill("Not empty").show()
print("______ADD COLUMN WITH CHANGES________")
df3.withColumn("New name", trim(col("Name"))).show()
print("________ RENAME VALUES_________")
df3.withColumn("new name", regexp_replace(col("Name"), "Nick", "Kolya")).show()
# change value 48 on 20 in column AGE
print("_________CHANGE AGE ___________")
df3.withColumn("new age", regexp_replace(col("Age"), "48", "20")).show()
print("_________ PRINT TEXT FROM TXT ____________")
# load data from txt file and csv
df4 = sc.textFile("C://Users/mchub/Desktop/fie.txt")
csvDf = sql.read.csv("C://Users/mchub/Desktop/file.csv", header=True, inferSchema=True)
listt = df4.collect()
for i in listt:
    print(i)
print("------------")

print("__________DELETE WHITESPACES_________")
new_df3 = df3.withColumn("name2", trim(col("Name")))

# Comparison two datasets
print("Dataset 5:")
df5.show()
print("Dataset 3:")
df3.show()
print("Exceptions:")
df3.exceptAll(df5).show()

# Join two tables
ta = df3.alias("ta")
tb = df5.alias("tb")
inner_join = ta.join(tb, ta.age == tb.age)
inner_join.show()
print("__________LEFT JOIN_________")
left_join = ta.join(tb, ta.age == tb.age, how='left')
left_join.show()
# Use filters
print("____________WITH FILTER__________#1")
left_join.filter(col('tb.name').isNull()).show()
print("____________WITH FILTER__________#2")
left_join.filter(col('tb.name').isNotNull()).show()

print("_______LOWER_______")
df3.withColumn("lower", lower(col("Name"))).show()

# Bad idea, it doesn`t work OK
print("______TEST CSVDF_________")
csvDf.show()
csvDf.printSchema()

# Lists of values to add in dataframe
number_list = [i for i in range(1, 900)]
time_list = [i for i in range(1, 1800, 2)]

# Random string to add in DataFrame


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))
word_list = [randomword(5) for i in range(1, 900)]

# new dataframe with values from lists
big_df = sql.createDataFrame(zip(number_list, time_list, word_list), ["id", "Time", "Name"])
number_of_rows = big_df.count()
big_df.show(number_of_rows - 700)
new_big_df = big_df.withColumn("Upper name", upper(col("Name")))
new_big_df.orderBy("id", ascending=False).show(number_of_rows - 700)
try:
    print("1")
#    new_big_df.write.option("header", "true").csv("C://Users/mchub/Desktop/bigdf.csv")
except:
    print("Oooops, error")
else:
    print("Success! File is saving on desktop. Path: C://Users/mchub/Desktop/bigdf.csv")

# Drop column
print("Demonstrate how DROP is working:")
df3.drop(col("Name")).show()
print("____ MAX VALUE IN COLUMN____")
max_value = new_big_df.agg({"Time": "max"}).collect()[0]
print(max_value)

# Statistical and Mathematical Functions
new_big_df.describe('id', 'Time', 'Name').show()

new_big_df.orderBy("Name", ascending=True).show(30)

print("______SHOW ONLY UNIQUE VALUES______")
new_big_df.distinct().show(200)

print("Group by key demonstrate:")
k = df3.withColumn("Name_trim", trim(col("Name")))
k = k.drop(col("Name"))
z = k.rdd.groupByKey()
for i in z.collect():
    print(i[0], [v for v in i[1]])  # 1

# New DF for test operations
df_test = sql.createDataFrame(
    [("20", "Nick", 10000), ("22", "Roman", 7000), ("22", "Marta", 4000), ("18", "Nastya", 3000), ("20", "Inna", 4000), ("20", "Ira", 7000), ("20", "Petya", 4000), ("22", "Ihor", 10000), ("18", "Zoya", 3000)],
    ["age", "name", "salary_per_year"])

# Group by key to show salary based on age/to show person name based on salary
print("DF_TEST")
df_test.show()
df_salary = df_test.select("salary_per_year", "name").rdd.groupByKey().collect()
df_age_salary = df_test.select("age", "salary_per_year").rdd.groupByKey().collect()
for i in df_salary:
    print("Salary: ", i[0], "| Names: ", [i for i in i[1]])
for i in df_age_salary:
    print("Age: ", i[0], "| Salary: ", [i for i in i[1]])

# Find avg of salaries
sum_df = df_test.select("salary_per_year")
avg_sal = sum_df.select(avg(col("salary_per_year"))).collect()[0][0]
print("Average salary: ", ceil(avg_sal))

# try to use map and filter methods
print("Persons with salary < 4000 dollars per year")
map_df = df_test.select("name", "salary_per_year").filter(col("salary_per_year") <= 4000)
map_df.show()
print("And I need to raise salary at 400 dollars...")
print("New salary:")
new_salary_df = map_df.withColumn("new_salary", col("salary_per_year")+400)
new_salary_df.show()
new_salary_df = new_salary_df.drop(col("salary_per_year"))

ta = new_salary_df.alias("sal_df")
tb = df_test.alias("df_test_sal")
inner_join = tb.join(ta, ta.name == tb.name)
inner_join.withColumn("raise", col("new_salary")-col("salary_per_year")).show()


# withColumn - add new column with changes
#
# orderBy("age", ascending = False) - sort
#
# to add some lists in dataframe - use function zip!
# * - unpack operator
# list(set(number_list).intersection(time_list)) extract elements of two lists
# Use .agg for functions: max, min, sum
# useful function dropDuplicates!
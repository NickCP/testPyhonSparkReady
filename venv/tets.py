from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, lit, col, exp, unix_timestamp, to_date
import time
from pyspark import SparkConf, SparkContext


conf = SparkConf().set('spark.driver.host', '127.0.0.1')
sc = SparkContext("local", "App Name", conf=conf)
# import datetime
# from pyspark.sql import types as t
# import os
# os.environ['HDFS_BASE_PATH'] = ''
# os.environ['HADOOP_HOST'] = ''
#
# import os
# import logging
# import functools
# from datetime import datetime
# from collections import OrderedDict, Counter
# from types import GeneratorType
# from decimal import Decimal
#
# from pyspark.sql import DataFrame
# from pyspark.sql import types as t
# from pyspark.sql.column import Column
# from pyspark.sql import functions as F
#
#
# from imported_meths import spark_helpers as helpers
# from imported_meths import unbundling_constants as const
# from imported_meths.spark_unbundling_helpers import *

spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

# def full_intersect(t1, t2):
#     if t2['earned_start_date'] <= t1['earned_start_date'] and t1['earned_end_date'] <= t2['earned_end_date']:
#         return t2
#     elif t1['earned_start_date'] <= t2['earned_start_date'] and t2['earned_end_date'] <= t1['earned_end_date']:
#         return t1

# def set_max_period(row, period):
#     if row['max_period_start']:
#         if period['earned_start_date'] < row['max_period_start'] and row['earned_end_date'] < period['max_end_start']:
#             row['max_period_start'], row['max_period_start'] = period['earned_start_date'], period['earned_end_date']
#         else:
#
#     else:
#         row['max_period_start'], row['max_period_start'] = period['earned_start_date'], period['earned_end_date']


def print_line():
    b = '*'
    print('\n')
    for x in range(0, 9):
        b = b + "*"
        print(b, end="")
        time.sleep(0.1)
    print('\n')
# df

df = spark.createDataFrame([
    [ 1, '2028-08-01', '2028-01-31'],
    [ 2, '2015-02-01', '2015-01-30'],
    [ 3, '2015-01-01', '2015-03-31'],
    [ 4, '2015-01-01', '2019-01-31'],
    [ 5, '2015-01-01', '2019-01-01'],
    [ 6, '2015-01-01', '2019-03-31'],
    [ 7, '2015-01-01', '2020-03-30'],
    [ 8, '2015-01-01', '2020-04-30.'],
    [ 9, '2015-01-01', '2020-05-30'],
    [10, '2015-01-01', '1998-06-30'],
], ['trx', 'earned_start_date', 'earned_end_date'])

df = df\
    .withColumn('earned_start_date', to_date(unix_timestamp(col('earned_start_date'), 'yyyy-MM-dd').cast("timestamp")))\
    .withColumn('earned_end_date', to_date(unix_timestamp(col('earned_end_date'), 'yyyy-MM-dd').cast("timestamp")))\
    .sort(['earned_end_date', 'earned_start_date'], ascending=[0, 1])
df.show()

rows = [{'trx': row['trx'], 'earned_start_date': row['earned_start_date'], 'earned_end_date': row['earned_end_date'],\
         'max_period': ''} for row in df.collect()]

i = 0
s = list(range(len(rows)))
list_of_unique_period = []
print_line()
while s:
    i = s[0]
    cur_row = rows[s[0]]
    for k, row in enumerate(rows[i:], start=i):
        if not row['max_period'] and cur_row['earned_start_date'] <= row['earned_start_date'] and row['earned_end_date'] <= cur_row['earned_end_date']:
            row['max_period'] = '%s_%s' % (cur_row['earned_start_date'], cur_row['earned_end_date'])
            s.remove(k)
    list_of_unique_period.append('%s_%s | TRX is: %s' % (cur_row['earned_start_date'], cur_row['earned_end_date'], cur_row['trx']))

for row in rows:
    print(row.items())
print_line()

for k, item_test in enumerate(list_of_unique_period):
    print("Item -", k+1, "- is: ", item_test)



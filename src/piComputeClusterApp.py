import time
from random import random

from pyspark.sql import SparkSession

slices = 10
numberOfThrows = 100000 * slices

print(
    f"About to throw {numberOfThrows} darts, ready? Stay away from the target!")
t0 = int(time.time() * 1000)

session = SparkSession.builder \
    .appName("PySpark Pi with lambdas") \
    .master("local[*]") \
    .getOrCreate()

t1 = int(time.time() * 1000)
print(f"Session initialized in {t1 - t0} ms")

l = [x for x in range(numberOfThrows)]
incrementalDf = session.sparkContext.parallelize(l)

t2 = int(time.time() * 1000)
print(f"Initial dataframe built in {t2 - t1} ms")

dartsRDD = incrementalDf.map(lambda i: 1 if (random() * 2 - 1) ** 2
                                            + (random() * 2 - 1) ** 2 <= 1 else 0)


t3 = int(time.time() * 1000)
print(f"Throwing darts done in  {t3 - t2} ms")

dartsInCircle = dartsRDD.reduce(lambda a, b: a+b)

t4 = int(time.time() * 1000)
print(f"\nAnalyzing result in  {t4 - t3} ms")
print(f"Pi is roughly {4.0 * dartsInCircle / numberOfThrows}")

session.stop()

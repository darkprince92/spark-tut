import collections

from pyspark import SparkConf, SparkContext

from src import config

conf = SparkConf().setMaster("local").setAppName("AgeFrieds")
sc = SparkContext(conf=conf)

FILE_LOC = config.DATA_LOCATION + "/fakefriends.csv"


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)


lines = sc.textFile(FILE_LOC)
rdd = lines.map(parse_line)

totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

results = average_by_age.collect()
sorted_results = collections.OrderedDict(sorted(results))
for result in sorted_results.items():
    print(result)

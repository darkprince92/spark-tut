from pyspark import SparkConf, SparkContext
import collections

FILE_LOC = "file:/Users/fahim/Desktop/spark-tut/data/ml-100k/u.data"
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile(FILE_LOC)
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

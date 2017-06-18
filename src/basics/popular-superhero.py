from pyspark import SparkContext, SparkConf
import codecs

conf = SparkConf().setMaster("local").setAppName("PopularHeros")
sc = SparkContext(conf=conf)

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
GRAPH_FILE = "file:" + DATA_LOCATION + "/marvel/Marvel-Graph.txt"
NAME_FILE = "file:" + DATA_LOCATION + "/marvel/Marvel-Names.txt"


def loadHeroNames():
    heroNames = {}
    with codecs.open(NAME_FILE) as f:
        try:
            for line in f:
                fields = line.split('\"')
                heroNames[int(fields[0])] = fields[1]
        except:
            pass
    return heroNames

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

def extractOccuranceCount(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)


heroNames = sc.textFile(NAME_FILE).map(parseNames)

lines = sc.textFile(GRAPH_FILE)
occuranceCount = lines \
    .map(extractOccuranceCount) \
    .reduceByKey(lambda x, y: x + y) \
    .map(lambda x: (x[1], x[0])) \
    .sortByKey()
    # .map(lambda x: (heroNames.lookup(x[1]), x[0]))

mostPopular = occuranceCount.max()
mostPopularName = heroNames.lookup(mostPopular[1])[0]

print("%s is most popular with count %d" % (mostPopularName, mostPopular[0]))
# results = occuranceCount.collect()
# for result in results:
#     print(result)

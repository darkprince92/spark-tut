from pyspark import SparkConf, SparkContext
import codecs

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
FILE_LOC = "file:" + DATA_LOCATION + "/ml-100k/u.data"


def extract_movies(line):
    fields = line.split()
    return (int(fields[1]), 1)


def loadMovieNames():
    movieNames = {}
    with codecs.open(DATA_LOCATION + "/ml-100k/u.ITEM", encoding="utf-8") as f:
        try:
            for line in f:
                fields = line.split('|')
                movieNames[int(fields[0])] = fields[1]
        except:
            pass
    return movieNames


movieNamesDict = sc.broadcast(loadMovieNames())

lines = sc.textFile(FILE_LOC)
# movies = lines.map(extract_movies)
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movie_count = movies.reduceByKey(lambda x, y: x + y)

sortedMovies = movie_count.map(lambda x: (x[1], x[0])).sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda x: (movieNamesDict.value.get(x[1], x[1]), x[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)

from pyspark.sql import SparkSession, Row, functions
import codecs

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
FILE_LOC = "file:" + DATA_LOCATION + "/ml-100k/u.data"


def load_movie_names():
    movie_names = {}
    with codecs.open(DATA_LOCATION + "/ml-100k/u.ITEM", encoding="utf-8") as f:
        try:
            for line in f:
                fields = line.split('|')
                movie_names[int(fields[0])] = fields[1]
        except:
            pass
    return movie_names


spark = SparkSession.builder.appName("SQLPopularMovies").getOrCreate()
name_dict = load_movie_names()

lines = spark.sparkContext.textFile(FILE_LOC)
movies = lines.map(lambda x: Row(movie_ID = int(x.split()[1])))

movies_dataset = spark.createDataFrame(movies)

top_movie_IDs = movies_dataset.groupBy("movie_ID").count().orderBy("count", ascending = False).cache()
top_movie_IDs.show()

top_10 = top_movie_IDs.take(10)

print(top_10)

spark.stop()
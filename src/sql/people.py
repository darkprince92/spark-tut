from pyspark.sql import SparkSession, Row
import collections

spark = SparkSession.builder.appName("SQLPeople").getOrCreate()

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
FILE_LOC = DATA_LOCATION + "/fakefriends.csv"


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=fields[1],
        age=int(fields[2]),
        num_of_friends=int(fields[3]))


lines = spark.sparkContext.textFile("file:" + FILE_LOC)
people = lines.map(mapper)

schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView('people')

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

print("Teenagers")
for teen in teenagers:
    print(teen)

schema_people.groupBy('age').count().orderBy('age').show()

spark.stop()

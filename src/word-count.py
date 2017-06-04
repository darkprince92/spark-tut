from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

DATA_LOCATION = 'file:/Users/fahim/PycharmProjects/spark-tut/data'
FILE_LOC = DATA_LOCATION + '/Book.txt'

lines = sc.textFile(FILE_LOC)
words = lines.flatMap(normalize_words)
word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_count_sorted = word_count.map(lambda x: (x[1], x[0])).sortByKey()

results = word_count_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if(word):
        print(word.decode() + ":\t\t" + count)

# print(word_count.items())

# for word, count in word_count_sorted.items():
#     clean_word = word.encode('ascii', 'ignore')
#     if(clean_word):
#         print(clean_word, count)
#

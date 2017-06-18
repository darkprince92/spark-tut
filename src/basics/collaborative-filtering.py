from pyspark import SparkConf, SparkContext
from math import sqrt
import codecs
import sys

conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
DATA_FILE = "file:" + DATA_LOCATION + "/ml-100k/u.data"


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


def filter_duplicates(rating_pair):
    # print("Joined Pair", rating_pair)
    (movie_id_1, ratings_1) = rating_pair[1][0]
    (movie_id_2, ratings_2) = rating_pair[1][1]
    return movie_id_1 < movie_id_2


def make_pairs(ratings_pair):
    (movie_id_1, ratings_1) = ratings_pair[1][0]
    (movie_id_2, ratings_2) = ratings_pair[1][1]
    return ((movie_id_1, movie_id_2), (ratings_1, ratings_2))


def compute_cosine_similarities(rating_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for rating_x, rating_y in rating_pairs:
        sum_xx += rating_x * rating_x
        sum_yy += rating_y * rating_y
        sum_xy += rating_x * rating_y
        num_pairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / float(denominator))

    return (score, num_pairs)


name_dict = load_movie_names()

data = sc.textFile(DATA_FILE)
# (user_id, (movie_id, rating))
ratings = data.map(lambda x: x.split()).map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))
# (user_id, ((movie_id_1, rating_1), (movie_id_2, rating_2))
# not sure here
joined_ratings = ratings.join(ratings)
unique_joined_ratings = joined_ratings.filter(filter_duplicates)
movie_pairs = unique_joined_ratings.map(make_pairs)
# (movie_id_1, movie_id_2) => (ratings_1, rating_2), (rating_1, rating_2), ...
movie_pair_ratings = movie_pairs.groupByKey()
movie_similarities = movie_pair_ratings.mapValues(compute_cosine_similarities).cache()

# Save similarity results
# movie_similarities.sortByKey()
# movie_similarities.saveAsTextFile(DATA_LOCATION + "/results/movie_similarities")

if(len(sys.argv) > 1):

    score_threshold = 0.97
    co_occurence_threshold = 0.50
    # print("Movie Names")
    # print(name_dict)

    movie_id = int(sys.argv[1])
    filtered_result = movie_similarities.filter(lambda x: \
                                                    (x[0][0] == movie_id or x[0][1] == movie_id) \
                                                    and x[1][0] > score_threshold \
                                                    and x[1][1] > co_occurence_threshold)

    results = filtered_result.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)
    print("Top ten similar movies for", name_dict[movie_id])
    for result in results:
        similarity = result[0]
        movie_pair = result[1]
        similar_movie_id = movie_pair[0] if movie_id == movie_pair[1] else movie_pair[1]
        similar_movie_name = similar_movie_id
        print(similar_movie_name, similarity)
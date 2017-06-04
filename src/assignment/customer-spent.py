from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerExpense")
sc = SparkContext(conf=conf)

DATA_LOCATION = 'file:/Users/fahim/PycharmProjects/spark-tut/data'
FILE_LOC = DATA_LOCATION + '/customer-orders.csv'


def parse_lines(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))


lines = sc.textFile(FILE_LOC)
customer_expense = lines.map(parse_lines)
total_expense = customer_expense.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = total_expense.collect()

for result in results:
    print("{:.2f}F".format(result[0]) + "\t" + str(result[1]))
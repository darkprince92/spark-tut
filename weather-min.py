from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WeahterMin")
sc = SparkContext(conf = conf)

'''Date Format: Weather-station-id, Date(yyyymmdd), observation-type, temperature-value'''

FILE_LOC = ""

parsed_lines = sc.textFile(FILE_LOC)
parsed_lines.filter(lambda x: 'TMIN' in x[1])

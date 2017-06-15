from pyspark import SparkContext, SparkConf
import codecs

conf = SparkConf().setMaster("local").setAppName("PopularHeros")
sc = SparkContext(conf=conf)

DATA_LOCATION = "/Users/fahim/PycharmProjects/spark-tut/data"
GRAPH_FILE = "file:" + DATA_LOCATION + "/marvel/Marvel-Graph.txt"
NAME_FILE = "file:" + DATA_LOCATION + "/marvel/Marvel-Names.txt"
INF = 9999
start_hero_id = 0
START_CHARACTER_ID = 5306
TARGET_CHARACTER_ID = 14


def load_hero_names():
    hero_names = {}
    with codecs.open(NAME_FILE) as f:
        try:
            for line in f:
                fields = line.split('\"')
                hero_names[int(fields[0])] = fields[1]
        except:
            pass
    return hero_names


def parse_names(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))


def extract_occurance_count(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)


hero_names = sc.textFile(NAME_FILE).map(parse_names)
hit_counter = sc.accumulator(0)


def convert_to_bfs(line):
    fields = line.split()
    hero_id = int(fields[0])
    connections = []

    for id in fields[1:]:
        connections.append(int(id))

    color = 0
    distance = INF

    if hero_id == START_CHARACTER_ID:
        color = 1
        distance = 0

    return (hero_id, (connections, distance, color))


def process_node(node):
    # print("---------------------------------Node Id, node state------------------------------------")
    # print(node)
    # print(node[0])
    # print(node[1][2])
    results = []
    connections = node[1][0]
    distance = node[1][1]
    color = node[1][2]
    if node[1][2] == 1:
        for child in connections:
            results.append((child, ([], distance + 1, 1)))
            if child == TARGET_CHARACTER_ID:
                print("$$$$ found you", child)
                hit_counter.add(1)
        color = 2

    processed_node = (node[0], (node[1][0], node[1][1], color))
    results.append(processed_node)
    # print("@@@@@@@@@@@@@@@@@@@--results", results)
    return results


def reduce_nodes(data1, data2):
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$--Reduction")
    # print(data1)
    # print(data2)
    conn_1 = data1[0]
    conn_2 = data2[0]

    connections = []
    distance = INF
    color = 0

    if len(conn_1) > 0:
        connections.extend(conn_1)
    if len(conn_2) > 0:
        connections.extend(conn_2)

    distance = min(data1[1], data2[1], distance)
    color = max(data1[2], data2[2], color)

    data = (connections, distance, color)
    # print("#######-data")
    # print(data)
    return data


lines = sc.textFile(GRAPH_FILE)
graph = lines.map(convert_to_bfs)

while hit_counter.value < 1:
    # for i in range(0, 10):
    mapped = graph.flatMap(process_node)
    print("--------------------------------------------------\nIterating..total nodes: %d" % mapped.count())
    # print(mapped.collect()[0])
    graph = mapped.reduceByKey(reduce_nodes)
    print("##################################################")
    # print("Iteration Count: %d, reduced count: %d" % (i, graph.count()))
    # if hit_counter.value > 0:
    #     break

result = graph.lookup(TARGET_CHARACTER_ID)
print("Degree of Separation", result)

# TODO: check results for correctness

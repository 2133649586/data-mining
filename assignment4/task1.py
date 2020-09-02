from pyspark import SparkContext,SparkConf
import sys
import os
import json
import time
from graphframes import GraphFrame
from pyspark.sql import SparkSession,Row

# os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

def printf(p):
    print(list(p))


def get_pair(x,user_business_dict):
    pair_list = []
    for i in user_business_dict:
        if i != x[0]:
            item1 = set(x[1])
            item2 = set(user_business_dict[i])
            length = len(item1&item2)
            if length>=int(filter_threshold):
                pair_list.append(tuple(sorted([x[0],i])))
    return pair_list


filter_threshold = sys.argv[1]
input_file_path = sys.argv[2]
community_output_file_path = sys.argv[3]

# filter_threshold = 7
# input_file_path = "ub_sample_data.csv"
# community_output_file_path = "task1output.txt"

start = time.time()
sc = SparkContext(appName="inf553")
sparkSession = SparkSession(sc)
sc.setLogLevel("WARN")

file_raw = sc.textFile(input_file_path)
first_row = file_raw.first()
all_file = file_raw.filter(lambda s: s != first_row)
user_business_file = all_file.map(lambda s: s.split(",")).map(lambda s: (s[0],s[1]))
user_businesslist_file = user_business_file.groupByKey().map(lambda s: (s[0],list(s[1]))).persist()

user_business_dict = user_businesslist_file.collectAsMap()
user_pair = user_businesslist_file.flatMap(lambda s: get_pair(s,user_business_dict)).distinct().collect()


edge_list = []
vertex_list = []
for each in user_pair:
    edge_list.append(tuple([each[0],each[1]]))
    edge_list.append(tuple([each[1], each[0]]))
    vertex_list.append(each[0])
    vertex_list.append(each[1])
#remove same vertex
vertex_list = list(set(vertex_list))


edge = sc.parallelize(edge_list).toDF(["src","dst"])
vertex = sc.parallelize(vertex_list).map(lambda s: Row(id=s)).toDF(['id'])

graph = GraphFrame(vertex,edge)
community = graph.labelPropagation(maxIter=5).rdd.map(lambda s: (s[1],s[0])).groupByKey()\
    .map(lambda s: list(sorted(s[1]))).collect()

sorted_community = sorted(community,key=lambda s:(len(s),min(s)))

with open(community_output_file_path, 'w') as output_file:
    for i in sorted_community:
        output_file.writelines(str(i)[1:-1] + "\n")
    output_file.close()

end = time.time()
print ("Duration: %s Seconds"%(end-start))
































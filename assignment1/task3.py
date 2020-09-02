import sys
from pyspark import SparkContext
import json
from operator import add


input_file = sys.argv[1]
output_path = sys.argv[2]
partition_type = sys.argv[3]
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])

# input_file = "review.json"
# output_path = "output_file"
# partition_type = "customized"
# n_partitions = 4
# n = 800

resultfile = {}
sc = SparkContext(appName="inf553")
file = sc.textFile(input_file).map(lambda s: json.loads(s))
review_file = file.map(lambda s: (s["business_id"],1))

def hash_function(s):
    num = ord(s[0])%n_partitions
    return num

if partition_type == "customized":
    p_review_file = review_file.partitionBy(n_partitions,lambda s: hash_function(s))
    partitions_number = p_review_file.getNumPartitions()
    resultfile["n_partitions"] = partitions_number
    item_number = p_review_file.glom().map(lambda s: len(s)).collect()
    resultfile["n_items"] = item_number
    count_review_file = p_review_file.reduceByKey(add).filter(lambda s: s[1]>n).collect()
    resultfile["result"] = count_review_file

else:
    partitions_number = review_file.getNumPartitions()
    resultfile["n_partitions"] = partitions_number
    item_number = review_file.glom().map(lambda s: len(s)).collect()
    resultfile["n_items"] = item_number
    count_review_file = review_file.reduceByKey(add).filter(lambda s: s[1]>n).map(lambda s: [s[0],s[1]]).collect()
    resultfile["result"] = count_review_file

with open(output_path, 'w') as final:
    json.dump(resultfile, final)
final.close()
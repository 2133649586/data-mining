from pyspark import SparkContext,SparkConf
import sys
import json
import time
from random import sample
import re
import math

def cosine_func(user_id,business_id,user_profile,business_profile):
    result = 0
    if user_id in user_profile:
        if business_id in business_profile:
            item1 = set(user_profile[user_id])
            item2 = set(business_profile[business_id])
            dot_product = len(item1&item2)
            euclidean_distance = math.sqrt(len(item1))*math.sqrt(len(item2))
            result = dot_product/euclidean_distance
    return result

# predict_file_path = "test_review.json"
# model_file = "task2model.json"
# output_file_path = "output.json"

predict_file_path = sys.argv[1]
model_file = sys.argv[2]
output_file_path = sys.argv[3]

start = time.time()
sc = SparkContext(appName="inf553")
file_raw = sc.textFile(model_file).map(lambda s: json.loads(s))

# (file, file_content)
file = file_raw.map(lambda s: (s["file"],s["file_content"]))
# user_index
user_index_dict = file.filter(lambda s: s[0]=="user_index").map(lambda s: (s[1][0],s[1][1])).collectAsMap()
business_index_dict = file.filter(lambda s: s[0]=="business_index").map(lambda s: (s[1][0],s[1][1])).collectAsMap()
business_profile = file.filter(lambda s: s[0]=="business_profile").map(lambda s: (s[1][0],s[1][1])).collectAsMap()
user_profile = file.filter(lambda s: s[0]=="user_profile").map(lambda s: (s[1][0],s[1][1])).collectAsMap()

predict_file = sc.textFile(predict_file_path).map(lambda s: json.loads(s)).map(lambda s: (s["user_id"],s["business_id"]))
predict_trans_file = predict_file.filter(lambda s: (s[0] in user_index_dict) and (s[1] in business_index_dict)).map(lambda s: ((s[0],s[1]),(user_index_dict[s[0]],business_index_dict[s[1]])))
cosine_sim = predict_trans_file.map(lambda s: (s[0],cosine_func(s[1][0],s[1][1],user_profile,business_profile)))
recommand_file = cosine_sim.filter(lambda s: s[1]>=0.01).collect()

result = []
for i in recommand_file:
    new_dict = {}
    new_dict["user_id"] = i[0][0]
    new_dict["business_id"] = i[0][1]
    new_dict["sim"] = i[1]
    result.append(new_dict)

with open(output_file_path, 'w') as final:
    for each in result:
        final.writelines(json.dumps(each)+"\n")
    final.close()

end = time.time()
print ("Duration: %s Seconds"%(end-start))







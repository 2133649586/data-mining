from pyspark import SparkContext,SparkConf
import sys
import json
import time
from random import sample
import re
import math
from itertools import combinations

def printf(p):
    print(list(p))


def calculate_pair(x,business_dict):
    result = []
    for i in business_dict:
        item1 = set(business_dict[i])
        item2 = set(x)
        lenth = len(item1&item2)
        if lenth>=3:
            result.append(i)
    return result

def prearson_similarity(x,y,business_3user_dict):
    item_dict1 = {}
    item_dict2 = {}
    for i in business_3user_dict[x]:
        item_dict1[i[0]] = i[1]
    for i in business_3user_dict[y]:
        item_dict2[i[0]] = i[1]
    candidate_user = []
    for i in item_dict1:
        if i in item_dict2:
            candidate_user.append(i)
    result = 0
    if len(candidate_user)>=3:
        sum1 = 0
        sum2 = 0
        count = 0
        for i in candidate_user:
            sum1 = sum1+item_dict1[i]
            sum2 = sum2+item_dict2[i]
            count += 1

        avg1 = sum1/count
        avg2 = sum2/count

        dot_product = 0
        for i in candidate_user:
            dot_product = dot_product+(item_dict1[i]-avg1)*(item_dict2[i]-avg2)

        euclidean_distance1 = 0
        euclidean_distance2 = 0
        for i in candidate_user:
            euclidean_distance1 = euclidean_distance1 + math.pow((item_dict1[i] - avg1),2)
            euclidean_distance2 = euclidean_distance2 + math.pow((item_dict2[i] - avg2),2)

        if euclidean_distance1==0 or euclidean_distance2==0:
            result = 0
        else:
            result = dot_product/(math.sqrt(euclidean_distance1)*math.sqrt(euclidean_distance2))
    return result

def all_parameter(hash_fun_num,user_number):
    prime_number_range_for_m = [i for i in range(20000,30000) if prime_or_not(i)==True]
    prime_number_range_for_p = [i for i in range(40000, 50000) if prime_or_not(i) == True]
    p_a = sample(range(0,300000),hash_fun_num)
    p_b = sample(range(0,300000),hash_fun_num)
    m_value = 0
    for i in prime_number_range_for_m:
        if i >= 1*user_number:
            m_value = i
            break
    p_m = [m_value for i in range(0,hash_fun_num)]
    # p_p = sample(prime_number_range,hash_fun_num)
    p_p = sample(prime_number_range_for_p,hash_fun_num)
    put_together = zip(p_a, p_b, p_m, p_p)
    together_list = [i for i in put_together]
    return together_list

def generate_sig(business_line, parameter_list):
    all_business_minvalue = []
    for i in range(0, len(parameter_list)):  # for each hash function
        minvalue_list = []
        for j in business_line[1]:
            hash_value = ((j * parameter_list[i][0] + parameter_list[i][1]) % parameter_list[i][3]) % \
                         parameter_list[i][2]
            minvalue_list.append(hash_value)
        min_value = min(minvalue_list)
        all_business_minvalue.append(((business_line[0], i), min_value))
    return all_business_minvalue

def prime_or_not(x):
    if x<=1:
        return 0
    elif x == 2:
        return True
    else:
        count = 2
        while count<x:
            if x%count == 0:
                return False
            else:
                count+=1
        return True

def get_pair(x):
    pair_list = []
    for i in combinations(x, 2):
        pair_list.append(tuple(sorted(list(i))))
    return pair_list


def calculate_similarity(x, y):
    result = 0
    item1 = set(y[x[0]])
    item2 = set(y[x[1]])
    result = len(item1 & item2) / len(item1 | item2)
    return result



start = time.time()

# train_file_path = "train_review.json"
# model_file = "task3item.json"
# cf_type = "user_based"

train_file_path = sys.argv[1]
model_file = sys.argv[2]
cf_type = sys.argv[3]

sc = SparkContext(appName="inf553")
file_raw = sc.textFile(train_file_path).map(lambda s: json.loads(s))
all = file_raw.map(lambda s: (s["business_id"], s["user_id"], s["stars"])).persist()

# make index for all business_id
all_business = all.map(lambda s: s[0]).distinct().sortBy(lambda s: s).collect()
business_index = {}
for index, business_id in enumerate(all_business):
    business_index[business_id] = index

#make index for all user_id
all_user = all.map(lambda s: s[1]).distinct().sortBy(lambda s: s).collect()
user_index = {}
for index, user_id in enumerate(all_user):
    user_index[user_id] = index

inverse_business = {}
for index, business_id in enumerate(all_business):
    inverse_business[index] = business_id

inverse_user = {}
for index, user_id in enumerate(all_user):
    inverse_user[index] = user_id


#use index to replace   (business_id, user_id, stars)
all_index_file = all.map(lambda s: (business_index[s[0]],user_index[s[1]],s[2])).persist()

if cf_type == "item_based":
    business_3user = all_index_file.map(lambda s: (s[0],(s[1],s[2]))).groupByKey().map(lambda s:(s[0],list(s[1]))).filter(lambda s: len(s[1])>=3)
    business_3user_dict = business_3user.collectAsMap()
    business_file = all_index_file.map(lambda s: (s[0],s[1])).groupByKey().map(lambda s:(s[0],list(s[1]))).filter(lambda s: len(s[1])>=3).persist()
    business_dict = business_file.collectAsMap()
    business_candidate = business_file.map(lambda s: (s[0],calculate_pair(s[1],business_dict))).flatMapValues(lambda s: s)\
        .map(lambda s: tuple(sorted(s))).distinct()
    candidate_with_sim = business_candidate.map(lambda s: (s,prearson_similarity(s[0],s[1],business_3user_dict))).filter(lambda s: s[1]>0).collect()
    print(len(candidate_with_sim))
    result = []
    for i in candidate_with_sim:
        new_dict = {}
        new_dict["b1"] = inverse_business[i[0][0]]
        new_dict["b2"] = inverse_business[i[0][1]]
        new_dict["sim"] = i[1]

        result.append(new_dict)
    with open(model_file, 'w') as final:
        for each in result:
            final.writelines(json.dumps(each)+"\n")
        final.close()


if cf_type == "user_based":

    hash_function_number = 30
    band_number = 30
    user_number = len(all_user)

    #use index replace user_id
    business_user = all_index_file.map(lambda s: (s[1],s[0])).groupByKey().map(lambda s: (s[0],list(s[1]))).persist()
    parameter_list = all_parameter(hash_function_number, user_number)     #50 is hash function number;
    business_signiture = business_user.flatMap(lambda s: generate_sig(s,parameter_list))  #((business_id,hash_id), minhashvalue)

    #LSH
    # band_id, business: key      hash_id, minvalue : value
    same_band_business = business_signiture.map(lambda s: (((s[0][1]%band_number),s[0][0]),(s[0][1],s[1]))).groupByKey()\
        .map(lambda s: (tuple(sorted(list(s[1]))),s[0][1])).groupByKey().map(lambda s: (s[0],list(s[1])))\
        .filter(lambda s: len(s[1])>=2)
    # candidate = same_band_business.flatMap(lambda s: get_pair(s[1])).distinct().collect()

    #similarity
    business_user_dict = {}
    for i in business_user.collect():
        business_user_dict[i[0]] = i[1]

    #fix
    candidate = same_band_business.flatMap(lambda s: get_pair(s[1])).distinct().map(lambda s: (s,calculate_similarity(s,business_user_dict)))\
        .filter(lambda s: s[1]>=0.01).map(lambda s: tuple(sorted([s[0][0],s[0][1]])))
    print(len(candidate.collect()))

    print("candidate finish")
    user_3business = all_index_file.map(lambda s: (s[1],(s[0],s[2]))).groupByKey().map(lambda s:(s[0],list(s[1])))
    # user: [(business1, stars1),(business12, stars13)...]
    user_3business_dict = user_3business.collectAsMap()
    user_file = all_index_file.map(lambda s: (s[1],s[0])).groupByKey().map(lambda s:(s[0],list(s[1]))).filter(lambda s: len(s[1])>=3).persist()
    # user: [business1,business12,business4...]
    user_dict = user_file.collectAsMap()
    # business_candidate = user_file.map(lambda s: (s[0],calculate_pair(s[1],user_dict))).flatMapValues(lambda s: s)\
    #     .map(lambda s: tuple(sorted(s))).distinct().filter(lambda s: s in candidate_list)
    print("state2")
    candidate_with_sim = candidate.map(lambda s: (s,prearson_similarity(s[0],s[1],user_3business_dict))).filter(lambda s: s[1]>0).collect()
    print(len(candidate_with_sim))
    result = []
    for i in candidate_with_sim:
        new_dict = {}
        new_dict["u1"] = inverse_user[i[0][0]]
        new_dict["u2"] = inverse_user[i[0][1]]
        new_dict["sim"] = i[1]

        result.append(new_dict)
    with open(model_file, 'w') as final:
        for each in result:
            final.writelines(json.dumps(each)+"\n")
        final.close()



end = time.time()
print ("Duration: %s Seconds"%(end-start))




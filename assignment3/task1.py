from pyspark import SparkContext,SparkConf
import sys
from operator import add
from itertools import combinations
import json
import time
from random import sample

def printf(p):
    print(list(p))

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


# random choose the parameter of hash fuction
def all_parameter(hash_fun_num,user_number):
    prime_number_range = [i for i in range(0,30000) if prime_or_not(i)==True]
    p_a = sample(range(0,3000),hash_fun_num)
    p_b = sample(range(0,3000),hash_fun_num)
    m_value = 0
    for i in prime_number_range:
        if i >= 1*user_number:
            m_value = i
            break
    p_m = [m_value for i in range(0,hash_fun_num)]
    p_p = sample(prime_number_range,hash_fun_num)
    put_together = zip(p_a, p_b, p_m, p_p)
    together_list = [i for i in put_together]
    return together_list


def generate_sig(business_line, parameter_list):
        all_business_minvalue = []
        for i in range(0,len(parameter_list)):     #for each hash function
            minvalue_list =[]
            for j in business_line[1]:
                hash_value = ((j*parameter_list[i][0]+parameter_list[i][1])%parameter_list[i][3])%parameter_list[i][2]
                minvalue_list.append(hash_value)
            min_value = min(minvalue_list)
            all_business_minvalue.append(((business_line[0],i),min_value))
        return all_business_minvalue


def get_pair(x):
    pair_list=[]
    for i in combinations(x,2):
        pair_list.append(tuple(sorted(list(i))))
    return pair_list


def calculate_similarity(x,y):
    result = 0
    item1 = set(y[x[0]])
    item2 = set(y[x[1]])
    result = len(item1&item2)/len(item1|item2)
    return result




#main file
# start = time.time()

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]

# input_file_path = "train_review.json"
# output_file_path = "output.txt"
hash_function_number = 1000
band_number = 21

sc = SparkContext(appName="inf553")
file_raw = sc.textFile(input_file_path).map(lambda s: json.loads(s))
all = file_raw.map(lambda s: (s["business_id"],s["user_id"],s["stars"]))

#ger all user_id
all_user = all.map(lambda s: s[1]).distinct().sortBy(lambda s: s).collect()
user_number = len(all_user)
user_index = {}
for index, user_id in enumerate(all_user):
    user_index[user_id] = index

#use index replace user_id
business_user = all.map(lambda s: (s[0],user_index[s[1]])).groupByKey().map(lambda s: (s[0],list(s[1])))
parameter_list = all_parameter(hash_function_number, user_number)     #50 is hash function number;
business_signiture = business_user.flatMap(lambda s: generate_sig(s,parameter_list))  #((business_id,hash_id), minhashvalue)

#LSH
# band_id, business: key      hash_id, minvalue : value
same_band_business = business_signiture.map(lambda s: (((s[0][1]%band_number),s[0][0]),(s[0][1],s[1]))).groupByKey()\
    .map(lambda s: (tuple(sorted(list(s[1]))),s[0][1])).groupByKey().map(lambda s: (s[0],list(s[1])))\
    .filter(lambda s: len(s[1])>=2)
candidate = same_band_business.flatMap(lambda s: get_pair(s[1])).distinct().collect()

#similarity
business_user_dict = {}
for i in business_user.collect():
    business_user_dict[i[0]] = i[1]

result_list = []
for i in candidate:
    pair = {}
    sim = calculate_similarity(i,business_user_dict)
    if sim >= 0.05:
        pair["b1"] = i[0]
        pair["b2"] = i[1]
        pair["sim"] = sim
        result_list.append(pair)


with open(output_file_path, 'w') as output_file:
    for i in result_list:
        output_file.writelines(json.dumps(i) + "\n")
    output_file.close()

# print(len(candidate))
# printf(all_user)

# end = time.time()
# print ("Duration: %s Seconds"%(end-start))










from pyspark import SparkContext
import sys
import os
import csv
import json
import math
import time
import random
from random import sample
from binascii import hexlify
from pyspark.streaming import StreamingContext
from datetime import datetime


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

def all_parameter(hash_fun_num,array_number):
    random.seed(888)
    prime_number_range_for_m = [i for i in range(0,20000) if prime_or_not(i)==True]
    prime_number_range_for_p = [i for i in range(30000, 40000) if prime_or_not(i) == True]
    p_a = random.sample(range(0,30000),hash_fun_num)
    p_b = random.sample(range(0,30000),hash_fun_num)
    m_value = 0
    for i in prime_number_range_for_m:
        if i >= 1*array_number:
            m_value = i
            break
    p_m = [m_value for i in range(0,hash_fun_num)]
    p_p = random.sample(prime_number_range_for_p,hash_fun_num)
    put_together = zip(p_a, p_b, p_m, p_p)
    together_list = [i for i in put_together]
    return together_list


def hash_calculate(x,parameter_list):
    all_hash_value = []
    for parameter in parameter_list:
        hash_value = ((x * parameter[0] + parameter[1]) % parameter[3]) % parameter[2]
        all_hash_value.append(hash_value)
    return all_hash_value


def Flajolet_Martin(x,parameter_list,small_group):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total = x.collect()
    list_for_count = []
    list_for_max = []
    total_hash_value = {}

    for a in range(0, len(parameter_list)):
        total_hash_value[a] = []

    for each in total:
        if each not in list_for_count:
            list_for_count.append(each)

        transfer_data = int(hexlify(each.encode('utf8')),16)
        # [h1_value,h2_value...]
        all_hash_value = hash_calculate(transfer_data,parameter_list)

        #[3,6,89,56,4,3,56,78,9]  number of trail
        all_trail_value = []
        for i in all_hash_value:
            new_value = bin(i)[2:]
            tail_value = len(new_value)-len(new_value.rstrip("0"))
            all_trail_value.append(tail_value)

        for i in range(0,len(parameter_list)):
            total_hash_value[i].append(all_trail_value[i])

    for i in total_hash_value:
        max_tail_value = max(total_hash_value[i])
        list_for_max.append(2 ** max_tail_value)

    #separete to 5 group, use average in each group, and use the median
    group_value_number = math.ceil(len(list_for_max)/small_group)
    group_result = []

    for i in range(0,len(list_for_max),group_value_number):
        target_list = list_for_max[i:i+group_value_number]
        average = sum(target_list)/group_value_number
        group_result.append(average)
    group_result = sorted(group_result)

    median = group_result[int(len(group_result)/2)]
    result = [current_time,len(list_for_count),int(median)]

    f = open(output_file_path, "a")
    csv_writer = csv.writer(f)
    csv_writer.writerow(result)
    f.close()



number_of_port = int(sys.argv[1])
output_file_path = sys.argv[2]

# number_of_port = 9999
# output_file_path = "task2output.csv"

#for hash function
hash_function_number = 32
array_number = 200
small_group = 4

sc = SparkContext(appName="inf553")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,5)

f = open(output_file_path,"w")
csv_writer = csv.writer(f)
csv_writer.writerow(["Time","Ground Truth","Estimation"])
f.close()

parameter_list = all_parameter(hash_function_number, array_number)

streamming = ssc.socketTextStream("localhost",number_of_port).window(30,10)
get_data = streamming.map(json.loads).map(lambda s: s["city"]).filter(lambda s: s!="" and s!=None)\
    .foreachRDD(lambda s: Flajolet_Martin(s,parameter_list,small_group))

ssc.start()
ssc.awaitTermination()




























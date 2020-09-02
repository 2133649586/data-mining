from pyspark import SparkContext
import sys
import os
import json
import time
import binascii
from random import sample
from binascii import hexlify


def printf(p):
    print(list(p))

def transfer(x):
    result = int(hexlify(x.encode('utf8')),16)
    return result

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

def all_parameter(hash_fun_num,user_number):
    prime_number_range_for_m = [i for i in range(0,30000) if prime_or_not(i)==True]
    prime_number_range_for_p = [i for i in range(30000, 50000) if prime_or_not(i) == True]
    p_a = sample(range(0,30000),hash_fun_num)
    p_b = sample(range(0,30000),hash_fun_num)
    m_value = 0
    for i in prime_number_range_for_m:
        if i >= 1*user_number:
            m_value = i
            break
    p_m = [m_value for i in range(0,hash_fun_num)]
    p_p = sample(prime_number_range_for_p,hash_fun_num)
    put_together = zip(p_a, p_b, p_m, p_p)
    together_list = [i for i in put_together]
    return together_list

def hash_calculate(x,parameter_list):
    all_hash_value = []
    if x == None or x == "":
        all_hash_value.append("no")
    else:
        for parameter in parameter_list:
            hash_value = ((x * parameter[0] + parameter[1]) % parameter[3]) % parameter[2]
            all_hash_value.append(hash_value)
    return all_hash_value

def hash_calculate2(x,parameter_list):
    all_hash_value = []
    if x == None or x == "":
        all_hash_value.append("no")
    else:
        x = int(hexlify(x.encode('utf8')),16)
        for parameter in parameter_list:
            hash_value = ((x * parameter[0] + parameter[1]) % parameter[3]) % parameter[2]
            all_hash_value.append(hash_value)
    return all_hash_value

def predict(hash_value,bit_array):
    if hash_value[0] == "no":
        result = 0
    else:
        result = 1
        for each in hash_value:
            if each not in bit_array:
                result = 0
    return result


first_json_path = sys.argv[1]
second_json_path = sys.argv[2]
output_file_path = sys.argv[3]

# first_json_path = "business_first.json"
# second_json_path = "business_second.json"
# output_file_path = "task1_output.txt"

hash_function_number = 50
array_number = 7000

start = time.time()
sc = SparkContext(appName="inf553")
sc.setLogLevel("WARN")

row_file = sc.textFile(first_json_path).map(lambda s: json.loads(s))
all_city_infirst = row_file.map(lambda s: s["city"]).filter(lambda s: s!="").distinct()
transfered_file = all_city_infirst.map(lambda s: transfer(s))

parameter_list = all_parameter(hash_function_number, array_number)
bit_array = transfered_file.flatMap(lambda s: hash_calculate(s,parameter_list)).distinct().collect()

predict_file = sc.textFile(second_json_path).map(lambda s: json.loads(s))
predict_result = predict_file.map(lambda s: s["city"]).map(lambda s: predict(hash_calculate2(s,parameter_list),bit_array)).collect()

print(predict_result)




end = time.time()
print ("Duration: %s Seconds"%(end-start))

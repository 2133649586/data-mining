from pyspark import SparkContext,SparkConf
import sys
import json
import time
from random import sample
import re
import math





def printf(p):
    print(list(p))

def transfer_index(x,user_index,business_index):
    if x[0] in user_index:
        if x[1] in business_index:
            result = tuple([user_index[x[0]],business_index[x[1]]])
        else:
            result = tuple(["no","no"])
    else:
        result = tuple(["no","no"])
    return result


def single_transfer_index(x,one_index):
    if x in one_index:
        return one_index[x]
    else:
        return "no"



def predict_function(user_id,business_id,user_b_score,model_dict,average_dict):
    #get all relative dict with b2 in (b1,b2,sim)  b2: sim


    user_score = user_b_score[user_id]  # [(b2,stars2),(b3,stars7),(b5,stars9)...]
    score_dict = {}
    for i in user_score:
        score_dict[i[0]] = i[1]


    new_pair = []
    # i in pair business_id
    for i in score_dict:
        pair = tuple(sorted([i,business_id]))
        if pair in model_dict:
            w = model_dict[pair]
            r = score_dict[i]
            new_pair.append([r,w])

    sorted_pair = sorted(new_pair, key=lambda x: x[1], reverse=True)
    # if len(sorted_pair) == 0:
    #     result = average_dict[business_id]
    #     return result
    a = 0
    b = 0
    for each in sorted_pair[:3]:
        a = a+each[0]*each[1]
        b = b + each[1]
    # print(sorted_pair)
    # print("a",a)
    # print("b",b)
    if (b == 0) or (a == 0):
        if business_id in average_dict:
            result = average_dict[business_id]
        else:
            result = 3.823989
    else:
        result = a / b
    return result



def predict_function2(user_id,business_id,business_u_score,model_dict,average_dict):

    business_score = business_u_score[business_id]
    score_dict = {}
    for i in business_score:
        score_dict[i[0]] = i[1]

    new_pair = []
    # i in pair business_id
    for i in score_dict:
        pair = tuple(sorted([i,user_id]))
        if pair in model_dict:
            w = model_dict[pair]
            r = score_dict[i]
            new_pair.append((i,[r,w]))

    sorted_pair = sorted(new_pair, key=lambda x: x[1][1], reverse=True)
    target_pair = sorted_pair[:3]
    target_dict = {}
    for i in target_pair:
        target_dict[i[0]] = i[1]

    # target_user_count = 0
    # target_user_sum = 0
    # for x in user_b_score[user_id]:
    #     target_user_count += 1
    #     target_user_sum = target_user_sum + x[1]
    # target_user_avg = target_user_sum / target_user_count


    a = 0
    b = 0
    for each in target_dict:
        if each in average_dict:
            a = a+((target_dict[each])[0]-average_dict[each])*(target_dict[each])[1]
        else:
            a = a + ((target_dict[each])[0] - 3.823989) * (target_dict[each])[1]
        b = b + (target_dict[each])[1]

    if b == 0 or a == 0:
        if user_id in average_dict:
            result = average_dict[user_id]
        else:
            result = 3.823989
    else:
        if user_id in average_dict:
            result = average_dict[user_id] + a / b
        else:
            result = 3.823989 + a / b
    return result






start = time.time()
#
train_file_path = sys.argv[1]
test_file_path = sys.argv[2]
model_file = sys.argv[3]
output_file = sys.argv[4]
cf_type = sys.argv[5]

business_average = "../resource/asnlib/publicdata/business_avg.json"
user_average = "../resource/asnlib/publicdata/user_avg.json"

# train_file_path = "train_review.json"
# test_file_path = "test_review.json"
# model_file = "task3user.model"
# output_file = "task3user.predict"
# cf_type = "user_based"
#
#
# business_average = "business_avg.json"
# user_average = "user_avg.json"



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
    file_model_raw = sc.textFile(model_file).map(lambda s: json.loads(s))
    #((user1,user2),sim)
    model_data = file_model_raw.map(lambda s: (tuple(sorted((business_index[s["b1"]],business_index[s["b2"]]))),s["sim"])).collectAsMap()

    file_test_raw = sc.textFile(test_file_path).map(lambda s: json.loads(s))
    test_data = file_test_raw.map(lambda s: (s["user_id"],s["business_id"])).map(lambda s: transfer_index(s,user_index,business_index)).filter(lambda s: s[0]!="no")
    # print(len(test_data.collect()))
    # user_id,[(business,stars),(),()...]
    user_with_business_score_dict = all_index_file.map(lambda s: (s[1],(s[0],s[2]))).groupByKey().map(lambda s:(s[0],list(s[1]))).collectAsMap()

    #aveg_dict
    average_dict = sc.textFile(business_average).map(lambda s: json.loads(s)).map(lambda s: dict(s)).flatMap(lambda s: s.items())\
        .filter(lambda s: s[0]!="UNK").map(lambda s: (single_transfer_index(s[0],business_index),s[1])).filter(lambda s: s[0]!="no").collectAsMap()

    predict_data = test_data.map(lambda s: (s,predict_function(s[0],s[1],user_with_business_score_dict,model_data,average_dict))).collect()

    result = []
    for i in predict_data:
        new_dict = {}
        new_dict["user_id"] = inverse_user[i[0][0]]
        new_dict["business_id"] = inverse_business[i[0][1]]
        new_dict["stars"] = i[1]
        result.append(new_dict)
    with open(output_file, 'w') as final:
        for each in result:
            final.writelines(json.dumps(each)+"\n")
        final.close()

if cf_type == "user_based":
    file_model_raw = sc.textFile(model_file).map(lambda s: json.loads(s))
    # ((business1,business2),sim)
    model_data = file_model_raw.map(lambda s: (tuple(sorted((user_index[s["u1"]],user_index[s["u2"]]))),s["sim"])).collectAsMap()

    file_test_raw = sc.textFile(test_file_path).map(lambda s: json.loads(s))
    test_data = file_test_raw.map(lambda s: (s["user_id"],s["business_id"])).map(lambda s: transfer_index(s,user_index,business_index)).filter(lambda s: s[0]!="no")

    # business_id,[(user,stars),(),()...]
    business_with_user_score_dict = all_index_file.map(lambda s: (s[0], (s[1], s[2]))).groupByKey().map(
        lambda s: (s[0], list(s[1]))).collectAsMap()

    # user_id,[(business,stars),(),()...]
    # user_with_business_score_dict = all_index_file.map(lambda s: (s[1],(s[0],s[2]))).groupByKey().map(lambda s:(s[0],list(s[1]))).collectAsMap()

    #aveg_dict
    average_dict = sc.textFile(user_average).map(lambda s: json.loads(s)).map(lambda s: dict(s)).flatMap(lambda s: s.items()).filter(lambda s: s[0]!="UNK")\
        .map(lambda s: (single_transfer_index(s[0],user_index),s[1])).filter(lambda s: s[0]!="no").collectAsMap()

    predict_data = test_data.map(
        lambda s: (s, predict_function2(s[0], s[1], business_with_user_score_dict, model_data, average_dict))).collect()

    result = []
    for i in predict_data:
        new_dict = {}
        new_dict["user_id"] = inverse_user[i[0][0]]
        new_dict["business_id"] = inverse_business[i[0][1]]
        new_dict["stars"] = i[1]
        result.append(new_dict)
    with open(output_file, 'w') as final:
        for each in result:
            final.writelines(json.dumps(each) + "\n")
        final.close()



end = time.time()
print ("Duration: %s Seconds"%(end-start))
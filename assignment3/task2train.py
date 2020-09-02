from pyspark import SparkContext,SparkConf
import sys
import json
import time
from random import sample
import re
import math

def printf(p):
    print(list(p))

def get_all_word(x,stopwords):
    tf_list =[]
    tf_store = {}
    new_text_list = []
    for each_text in x:
        new_list = []
        each_text = each_text.replace("\n"," ")
        remove_chars = '[0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        no_punc_text = re.sub(remove_chars, ' ', each_text)
        divide_text = no_punc_text.split(" ")
        for i in divide_text:
            i = i.lower()
            if i != "":
                if i not in stopwords:
                    new_list.append(i)
        new_text_list.extend(new_list)

    for i in new_text_list:
        if i in tf_store:
            tf_store[i] += 1
        else:
            tf_store[i] = 1

    max_key = max(tf_store.keys(),key=(lambda x:tf_store[x]))
    max_fre = tf_store[max_key]

    for i in tf_store:
        if tf_store[i]>=4:
            tf_list.append((i,tf_store[i]/max_fre))

    return tf_list

def transfer(x):
    for i in range(0,len(x)):
        x[i] = x[i][0]
    return x

def change(x,business_profile):
    result = []
    for i in x:
       if i in business_profile:
        result.extend(business_profile[i])
    result = list(set(result))
    return result


# input_file_path = "train_review.json"
# model_file = "task2model.json"
# stopword_path = "stopwords"

input_file_path = sys.argv[1]
model_file = sys.argv[2]
stopword_path = sys.argv[3]

start = time.time()
stopwords = []
with open(stopword_path) as read_file:
    for line in read_file:
        stopwords.append(line.strip())

sc = SparkContext(appName="inf553")
file_raw = sc.textFile(input_file_path).map(lambda s: json.loads(s))
all = file_raw.map(lambda s: (s["business_id"],s["user_id"],s["text"])).persist()

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

all_index_file = all.map(lambda s: (business_index[s[0]],user_index[s[1]],s[2])) # cost 15s


#split every word in business id (business_id,[text1,text2...])
# put (business_id, (word, tf))
business_id_text = all_index_file.map(lambda s: (s[0],s[2])).groupByKey().map(lambda s: (s[0],list(s[1])))\
    .filter(lambda s: len(s[1])>0).flatMapValues(lambda s: get_all_word(s,stopwords)).persist()

#IDF (word, IDF)
N = len(business_index)
business_idf_dict = business_id_text.map(lambda s: (s[1][0],s[0])).groupByKey().map(lambda s: (s[0],math.log(N/len(s[1]),2)))\
    .collectAsMap()

#TFIDF  (business_id,(word,TFIDF))
#put (business_id,[word1,word2...])
business_tfidf = business_id_text.map(lambda s: (s[0],(s[1][0],s[1][1]*business_idf_dict[s[1][0]])))
business_top200 = business_tfidf.groupByKey().mapValues(lambda s: sorted(s, key=lambda x: x[1], reverse=True))\
    .map(lambda s: (s[0],transfer(s[1][:200])))

all_word = business_top200.flatMap(lambda s: [i for i in s[1]]).distinct().sortBy(lambda s: s).collect()
word_index = {}
for index, word in enumerate(list(all_word)):
    word_index[word] = index


#business_profile
business_profile = business_top200.map(lambda s: (s[0],[word_index[i] for i in s[1]])).collectAsMap()
# user_profile (remove duplicate word)
#input (business_id, user_id)
user_profile = all_index_file.map(lambda s:(s[1],s[0])).groupByKey().filter(lambda s: len(s[1])>0)\
    .map(lambda s: (s[0],list(set(list(s[1]))))).map(lambda s: (s[0],change(s[1],business_profile)))\
    .collectAsMap()

# print(user_profile)
# write business_index,
output_dict = []
for i in user_index:
    new_dict = {}
    new_dict["file_content"] = [i,user_index[i]]
    new_dict["file"] = "user_index"
    output_dict.append(new_dict)

for i in business_index:
    new_dict = {}
    new_dict["file_content"] = [i,business_index[i]]
    new_dict["file"] = "business_index"
    output_dict.append(new_dict)

for i in user_profile:
    new_dict = {}
    new_dict["file_content"] = [i,user_profile[i]]
    new_dict["file"] = "user_profile"
    output_dict.append(new_dict)

for i in business_profile:
    new_dict = {}
    new_dict["file_content"] = [i,business_profile[i]]
    new_dict["file"] = "business_profile"
    output_dict.append(new_dict)

# output_dict["user_index"] = user_index
# output_dict["business_index"] = business_index
# # output_dict["word_index"] = word_index
# output_dict["business_profile"] = business_profile
# output_dict["user_profile"] = user_profile

with open(model_file, 'w') as final:
    for each in output_dict:
        final.writelines(json.dumps(each)+"\n")
    final.close()


end = time.time()
print ("Duration: %s Seconds"%(end-start))


import sys
from pyspark import SparkContext
import json
from operator import add
import re

def printf(p):
    print(list(p))


# file_name = "review.json"
# given_year = 2018
# given_number_m = 8
# given_number_n = 10
# output_path = "output_file"
# stopword_file_path = "stopwords"

file_name = sys.argv[1]
given_year = int(sys.argv[4])
given_number_m = int(sys.argv[5])
given_number_n = int(sys.argv[6])
output_path = sys.argv[2]
stopword_file_path = sys.argv[3]

resultfile = {}
sc = SparkContext(appName="inf553")
file = sc.textFile(file_name).map(lambda s: json.loads(s))

# task A
eachline = file.count()
resultfile["A"] = int(eachline)

# task B
eachline = file.map(lambda s: (s["date"])).filter(lambda s: int(s[0:4])==given_year).count()
resultfile["B"] = int(eachline)

# task C
eachline = file.map(lambda s: s["user_id"]).distinct().count()
resultfile["C"] = int(eachline)

# task D
eachline = file.map(lambda s: (s["user_id"],1)).reduceByKey(add)
line = eachline.map(lambda s: [s[1],s[0]]).sortByKey(False).map(lambda s: [s[1],s[0]]).takeOrdered(given_number_m, key=lambda s: (-s[1], s[0]))
# line = eachline.sortBy(lambda s: (-s[1], s[0]), ascending=True).take(given_number_m)
resultfile["D"] = line

# task E
def replace_data(data):
    for i in data:
        if i in punctuation:
            data = data.replace(i," ")
    data = data.replace("\n"," ")
    return data

stopword_file = open(stopword_file_path,"r").readlines()
stopword = []
for i in stopword_file:
    stopword.append(i[:-1])
# punctuation = "([,.!?:;])"
punctuation = ["(","[",",",".","!","?",":",";","]",")"]

eachline = file.map(lambda s: (s["text"]))
#line = eachline.flatMap(lambda s: (re.sub(f"{punctuation}",'',s)).replace("\n"," ").split(" ")).map(lambda s: (s,1)).reduceByKey(add)
line = eachline.flatMap(lambda s: replace_data(s).split(" ")).map(lambda s: (s.lower(),1)).reduceByKey(add)
consequence = line.filter(lambda s: s[0] != "").filter(lambda s: s[0] != None).filter(lambda s: (s[0]).lower() not in stopword).\
takeOrdered(given_number_n, key=lambda s: (-s[1], s[0]))
new_file = sc.parallelize(consequence)
end = new_file.map(lambda s: s[0]).collect()
resultfile["E"] = end

# print(result_dict)

with open(output_path, 'w') as final:
    json.dump(resultfile, final)
final.close()









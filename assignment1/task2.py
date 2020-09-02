import sys
from pyspark import SparkContext
import json
from operator import add

# review_file_name = "review.json"
# business_file_name = "business.json"
# output_path = "output_file"
# if_spark = "spark"
# n = 10

review_file_name = sys.argv[1]
business_file_name = sys.argv[2]
output_path = sys.argv[3]
if_spark = sys.argv[4]
n = int(sys.argv[5])

def printf(p):
    print(list(p))

resultfile = {}
new_resultfile = {}

if if_spark == "spark":
    sc = SparkContext(appName="inf553")
    review_file = sc.textFile(review_file_name).map(lambda s: json.loads(s))
    business_file = sc.textFile(business_file_name).map(lambda s: json.loads(s))

    each_review = review_file.map(lambda s: (s["business_id"],float(s["stars"])))
    separate_review = each_review.groupByKey().map(lambda s: (s[0],list(s[1]))).map(lambda s: (s[0],[len(s[1]),sum(s[1])]))

    each_business = business_file.map(lambda s: (s["business_id"],s["categories"]))
    separate_business = each_business.flatMapValues(lambda s: str(s).split(", "))


    total_file = separate_business.leftOuterJoin(separate_review)
    divide_file = total_file.map(lambda s: (s[1][0],s[1][1])).filter(lambda s: s[1]!=None).filter(lambda s:s[1]!="")\
        .reduceByKey(lambda x,y: [x[0]+y[0], x[1]+y[1]])
    result = divide_file.map(lambda s: (s[0], s[1][1]/s[1][0])).map(lambda s: (s[1],s[0])).sortByKey(False)\
        .map(lambda s: [s[1],s[0]]).takeOrdered(n, key=lambda s: (-s[1], s[0]))

    resultfile["result"] = result


if if_spark == "no_spark":

    review_file = {}
    lines = open(review_file_name, encoding="utf8").readlines()
    for line in lines:
        each_line = json.loads(line)
        extra_file = [each_line["business_id"], each_line["stars"]]
        if extra_file[0] not in review_file:
            review_file[extra_file[0]] = [extra_file[1],1]
        else:
            review_file[extra_file[0]] = [review_file[extra_file[0]][0]+extra_file[1],review_file[extra_file[0]][1]+1]
    # print(review_file)

    business_file = {}
    lines = open(business_file_name, encoding="utf8").readlines()
    for line in lines:
        each_line = json.loads(line)
        extra_file = [each_line["business_id"],each_line["categories"]]
        if (extra_file[0] != None) and (extra_file[1] != None):
            category = extra_file[1].split(", ")
            for i in category:
                if i not in business_file:
                    business_file[i] = [str(extra_file[0])]
                else:
                    business_file[i].append(extra_file[0])
    # print(business_file)


    for i in business_file:
        sum = 0
        count = 0
        for j in business_file[i]:
            if j in review_file:
                sum = sum+review_file[j][0]
                count = count+review_file[j][1]
        if sum!=0 and count!=0:
            avg = sum/count
            resultfile[i]= avg

    output = sorted(resultfile.items(),key=lambda x:(-x[1],x[0]))

    num = 0
    result_list = []
    for x in output:
        if num<n:
            result_list.append([x[0],x[1]])
            num += 1
    new_resultfile["result"] = result_list
    resultfile = new_resultfile

with open(output_path, 'w') as final:
    json.dump(resultfile, final)
final.close()





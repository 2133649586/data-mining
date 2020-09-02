from pyspark import SparkContext
import json
import csv

review_path = "review.json"
business_path = "business.json"
output_path = "user_business.csv"

def printf(p):
    print(list(p))

sc = SparkContext(appName="inf553")
business_file = sc.textFile(business_path).map(lambda s: json.loads(s)).map(lambda s:(s["business_id"],s["state"]))\
    .filter(lambda s: s[1]=="NV").map(lambda s: s[0]).collect()

review_file = sc.textFile(review_path).map(lambda s: json.loads(s)).map(lambda s: (s["user_id"],s["business_id"]))\
    .filter(lambda s: s[1] in business_file).collect()

f = open(output_path,"w",encoding='utf-8',newline='' "")
csv_writer = csv.writer(f)
csv_writer.writerow(["user_id","business_id"])
for i in review_file:
    csv_writer.writerow([i[0],i[1]])
f.close()













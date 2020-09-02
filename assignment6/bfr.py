from pyspark import SparkContext
import sys
import os
import csv
import json
import math
import time
import random

# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

def printf(p):
    print(list(p))



def calculate_Euclidean(point1,point2):
    sum = 0
    result = 0
    for i in range(0,len(point1)):
        sum = sum+(point1[i]-point2[i])*(point1[i]-point2[i])
    result = math.sqrt(sum)
    return result



def belong_which_cluster(datapoint,centroid_dict):
    # datapoint: [32.53456345354,76.786876687686,76.23463465434]
    belong = {}
    for i in centroid_dict:
        distance = calculate_Euclidean(datapoint,centroid_dict[i])
        belong[i] = distance
    belong_cluster = min(belong, key=belong.get)
    return belong_cluster


#return:    {0:[(1,[23.2345235635,23.32453,76.23424]),(),(),()],   1:[(),(),(),()]...}
def Kmean(data_dict,n_cluster):

    #find random centroid
    centroid_dict = {}      # {0:[32.53456345354,76.786876687686,76.23463465434], 1:[32.53456345354,76.786876687686,76.23463465434]}
    centroid_list = []
    count_dict = {}
    for i in data_dict:
        count_dict[i] = float("inf")
    for i in range(0,n_cluster):
        if i == 0:
            # first_point = random.sample(list(data_dict.keys()),1)[0]
            first_point = list(data_dict.keys())[0]
            centroid_dict[i] = data_dict[first_point]
            # centroid_dict[i] = [first_point, data_dict[first_point]]
            centroid_list.append(first_point)
        else:
            # count_dict = {}
            for j in data_dict:
                # if j not in centroid_list:
                count_dict[j] = min((calculate_Euclidean(data_dict[j],centroid_dict[i-1])),count_dict[j])
            max_distance_point = max(count_dict,key=count_dict.get)    #point id
            centroid_dict[i] = data_dict[max_distance_point]
            centroid_list.append(max_distance_point)


    old_point_cluster = {}
    different_cluster_point = len(data_dict)
    iteration_num = 0
    cluster_dict = {}
    d = 0
    while different_cluster_point > len(data_dict)/100 and iteration_num < 15:
        #assign all data to centroid
        # cluster_dict : {0:[[1,[23.2345235635,23.32453,76.23424]],[],[],[]],1:[[],[],[],[]]...}
        different_cluster_point = 0
        point_cluster = {}
        cluster_dict = {}
        for i in centroid_dict:
            cluster_dict[i]=[]


        for i in data_dict:
            d = len(data_dict[i])
            belong = belong_which_cluster(data_dict[i],centroid_dict)
            cluster_dict[belong].append((i,data_dict[i]))
            point_cluster[i] = belong


        #calculate number of point change to different cluster
        if len(old_point_cluster) == 0:
            different_cluster_point = len(point_cluster)
        else:
            for i in point_cluster:
                if point_cluster[i]!=old_point_cluster[i]:
                    different_cluster_point += 1
        old_point_cluster = point_cluster


        #calculate new centroid
        # new_cluster_dict : {0:[23.234235,23.234234563,23.3456234,67.34534],1:[]...}
        # cluster_dict : {0:[(1,[23.2345235635,23.32453,76.23424]),(),(),()],   1:[(),(),(),()]...}
        new_centroid_dict = {}
        for i in cluster_dict:
            #cluster_content  :  [(1,[23.2345235635,23.32453,76.23424]),(),(),()]
            cluster_content = cluster_dict[i]
            dimention_sum_dict = {}
            # j  :  (1,[23.2345235635,23.32453,76.23424])
            for j in cluster_content:
                for dimention in range(0,len(j[1])):
                    if dimention not in dimention_sum_dict:
                        dimention_sum_dict[dimention] = j[1][dimention]
                    else:
                        dimention_sum_dict[dimention] = dimention_sum_dict[dimention] + j[1][dimention]


            dimention_sum_list = []
            if dimention_sum_dict == {}:
                for item in range(0,d):
                    dimention_sum_list.append(0.0)
            else:
                for x in dimention_sum_dict:
                    dimention_sum_list.append(dimention_sum_dict[x]/len(cluster_content))


            new_centroid_dict[i] = dimention_sum_list


        centroid_dict = new_centroid_dict
        iteration_num += 1

    return cluster_dict


# return     n, sum, sumsq
def cluster_summarize(cluster_dict):
    summarize_dict = {}
    for i in cluster_dict:
        #对一个cluster里面所有的点做操作
        sum = []
        sumsq = []
        n = 0
        count = 0
        for j in cluster_dict[i]:
            point = j[1]
            if count == 0:
                for y in range(0,len(point)):
                    sum.append(0)
                    sumsq.append(0)
                count = 1
            for y in range(0,len(point)):
                sum[y] += point[y]
                sumsq[y] += point[y]*point[y]
            n += 1
        summarize_dict[i] = [n,sum,sumsq]
    return summarize_dict



def Mahalanobis_Distance(point,ds_centroid,ds_standard_deviation):
    # ds_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
    threshold = 2*math.sqrt(len(point))
    min_sum = 2*threshold
    result = "no"
    for cluster in ds_centroid:
        cluster_centroid = ds_centroid[cluster]
        cluster_deviation = ds_standard_deviation[cluster]

        sum = 0
        for i in range(0,len(point)):
            sum = sum + ((point[i]-cluster_centroid[i])/cluster_deviation[i])*((point[i]-cluster_centroid[i])/cluster_deviation[i])
        sum = math.sqrt(sum)
        if sum < threshold:
            if sum < min_sum:
                min_sum = sum
                result = cluster

    return result



def Mahalanobis_Distance2(point,ds_centroid,ds_standard_deviation):
    # ds_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
    min_sum = float("inf")
    result = "no"

    for cluster in ds_centroid:
        cluster_centroid = ds_centroid[cluster]
        cluster_deviation = ds_standard_deviation[cluster]

        # if cluster == 6:
        #     print(6)
        #     print("cluster_centroid",cluster_centroid)
        #     print("cluster_deviation",cluster_deviation)
        # if cluster == 7:
        #     print(7)
        #     print("cluster_centroid",cluster_centroid)
        #     print("cluster_deviation",cluster_deviation)

        sum = 0
        for i in range(0,len(point)):
            sum = sum + ((point[i]-cluster_centroid[i])/cluster_deviation[i])*((point[i]-cluster_centroid[i])/cluster_deviation[i])
        sum = math.sqrt(sum)

        if sum < min_sum:
            min_sum = sum
            result = cluster

    return result



input_path = sys.argv[1]
n_cluster = int(sys.argv[2])
out_file1 = sys.argv[3]
out_file2 = sys.argv[4]

# input_path = "/Users/liyifan/Desktop/553/inf553hw/hw6/data1"
# n_cluster = 10
# out_file1 = "output1.csv"
# out_file2 = "output2.csv"


start = time.time()
next_file_path = sorted(os.listdir(input_path))

sc = SparkContext(appName="inf553")
sc.setLogLevel("WARN")

sample_data = {}
left_data = {}
count_for_which = 0

rs_set = {}
rs_set_count = 0

cs_set = {}
cs_summerize = {}
cs_set_count = 0

ds_set = {}
ds_summerize = {}
ds_set_count = 0

ds_centroid = {}
ds_standard_deviation = {}
cs_centroid = {}
cs_standard_deviation = {}

count_for_file = 0

f2 = open(out_file2,"w")
csv_writer = csv.writer(f2)
csv_writer.writerow(["round_id","nof_cluster_discard","nof_point_discard","nof_cluster_compression","nof_point_compression","nof_point_retained"])
f2.close()

for index in range(0,len(next_file_path)):
    #通过第一个文件做初始化
    each_file = next_file_path[index]

    if index == 0:
        assemble_file_path = input_path+"/"+each_file
        file_raw = sc.textFile(assemble_file_path).map(lambda s: s.split(","))
        dimention_file = file_raw.map(lambda s: (int(s[0]),[float(i) for i in s[1:]])).persist()

        # dimention_dict = dimention_file.collectAsMap()
        #get 10% data from original data
        all_data = dimention_file.map(lambda s: s[0]).collect()
        sample_length = int(len(all_data)/5)
        if sample_length>10000:
            sample_data_point = all_data[:10000]
        else:
            random.seed(123)
            sample_data_point = random.sample(all_data,sample_length)

        #sample data, remain_data
        sample_data = dimention_file.filter(lambda s: s[0] in sample_data_point).collectAsMap()
        left_data = dimention_file.filter(lambda s: s[0] not in sample_data_point).collectAsMap()


        #use k*5 as centroid to do kmean for sample
        #k_mean的结果是： {0:[(34,[23.2345235, 23.32453, 76.23424]), (22,[]),(34435,[])], 1:[(),(),()]}
        # 对第一次结果设置为 RS 和 DS
        # DS:  {0:[(34,[23.2345235, 23.32453, 76.23424]), (22,[]),(34435,[])], 1:[(),(),()]}
        cluster_for_first = Kmean(sample_data,n_cluster*3)
        # for i in cluster_for_first:
        #     print(i,len(cluster_for_first[i]))

        for i in cluster_for_first:
            if len(cluster_for_first[i])<100:
                rs_set[rs_set_count] = cluster_for_first[i]
                rs_set_count += 1
            else:
                ds_set[ds_set_count] = cluster_for_first[i]
                ds_set_count += 1

###############################################################################
        #对 DS 非离群数据点在做Kmean 将其合并为k个聚类的 DS
        # DS中的所有的数据点
        new_data_point = {}
        for i in ds_set:
            for j in ds_set[i]:
                new_data_point[j[0]] = j[1]
        cluster_for_second = Kmean(new_data_point,n_cluster)

        #重置ds_set
        ds_set_count = 0
        ds_set ={}
        for i in cluster_for_second:
            ds_set[ds_set_count] = cluster_for_second[i]
            ds_set_count += 1

        for i in ds_set:
            print(i,len(ds_set[i]))

        ds_summerize = cluster_summarize(ds_set)


############################################################################
        # 对 RS 非离群数据点在做Kmean 将其分为RS 和 CS
        new_data_point = {}
        for i in rs_set:
            for j in rs_set[i]:
                new_data_point[j[0]] = j[1]
        cluster_for_second = Kmean(new_data_point,n_cluster*3)

        # 重置rs_set, 之前没有用到cs, cs_set不用重置
        rs_set_count = 0
        rs_set = {}
        for i in cluster_for_second:
            if len(cluster_for_second[i])>1:
                cs_set[cs_set_count] = cluster_for_second[i]
                cs_set_count += 1
            else:
                rs_set[rs_set_count] = cluster_for_second[i]
                rs_set_count += 1

        cs_summerize = cluster_summarize(cs_set)

#####################################################################################

        # 得到DS的类心
        # 得到DS的每个dimention的标准差
        #  ds_summerize : n, sum, sumsq
        #  ds_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
        #  ds_standard_deviation : {0:[0.345, 0.4567, 0.34567], 1:[]...}
        for i in ds_summerize:
            ds_centroid[i] = [each_item/ds_summerize[i][0] for each_item in ds_summerize[i][1]]

        for i in ds_summerize:
            summerize_each = ds_summerize[i]
            summerize_list = []
            for j in range(0,len(summerize_each[1])):
                a = summerize_each[2][j]/summerize_each[0]
                b = (summerize_each[1][j]/summerize_each[0]) * (summerize_each[1][j]/summerize_each[0])
                if (a-b)>0:
                    summerize_result = math.sqrt(a-b)
                else:
                    summerize_result = 1e-10
                summerize_list.append(summerize_result)
            ds_standard_deviation[i] = summerize_list


        # 得到CS的类心
        # 得到CS的每个dimention的标准差
        # cs_centroid = {}
        # cs_standard_deviation = {}

        #  cs_summerize : n, sum, sumsq
        #  cs_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
        #  cs_standard_deviation : {0:[0.345, 0.4567, 0.34567], 1:[]...}
        for i in cs_summerize:
            cs_centroid[i] = [each_item/cs_summerize[i][0] for each_item in cs_summerize[i][1]]

        for i in cs_summerize:
            summerize_each = cs_summerize[i]
            summerize_list = []
            for j in range(0,len(summerize_each[1])):
                a = summerize_each[2][j]/summerize_each[0]
                b = (summerize_each[1][j]/summerize_each[0]) * (summerize_each[1][j]/summerize_each[0])
                if (a-b)>0:
                    summerize_result = math.sqrt(a-b)
                else:
                    summerize_result = 1e-10
                summerize_list.append(summerize_result)
            cs_standard_deviation[i] = summerize_list














    if index != 0 or count_for_which == 0:
        #得到下一个文件的新数据点

        if count_for_which == 0:
            file_data = left_data
            count_for_which = 1
        else:
            assemble_file_path = input_path+"/"+each_file
            file_raw = sc.textFile(assemble_file_path).map(lambda s: s.split(","))
            dimention_file = file_raw.map(lambda s: (int(s[0]),[float(i) for i in s[1:]]))
            file_data = dimention_file.collectAsMap()

        total_list = list(file_data.keys())
        # DS 分类
        # print(ds_centroid)
        # print(ds_standard_deviation)
        # 计算每个点到DS 类心的距离，决定是否分配个DS
        # ds_set = {}
        # ds_list = []
        for i in file_data:
            put_which_cluster = Mahalanobis_Distance(file_data[i],ds_centroid,ds_standard_deviation)
            if put_which_cluster != "no":
                # ds_list.append(i)
                total_list.remove(i)
                if put_which_cluster in ds_set:
                    ds_set[put_which_cluster].append((i,file_data[i]))
                else:
                    ds_set[put_which_cluster]= [(i,file_data[i])]
#################################################################################################################

        # 原始数据中除分配给DS的数据之外的数据
        rest_data = {}
        # for i in file_data:
        #     if i not in ds_list:
        #         rest_data[i] = file_data[i]
        for i in total_list:
            rest_data[i] = file_data[i]

        # CS 分类
        # print(ds_centroid)
        # print(ds_standard_deviation)
        # 计算每个点到CS 类心的距离，决定是否分配个CS
        # cs_set = {}
        # cs_list = []
        for i in rest_data:
            put_which_cluster = Mahalanobis_Distance(rest_data[i],cs_centroid,cs_standard_deviation)
            if put_which_cluster != "no":
                # cs_list.append(i)
                total_list.remove(i)
                if put_which_cluster in cs_set:
                    cs_set[put_which_cluster].append((i,rest_data[i]))
                else:
                    cs_set[put_which_cluster] = [(i,rest_data[i])]

#################################################################################################################

        #原始数据中除DS和CS以外的数据
        #全部分配给RS
        last_data = {}
        # for i in file_data:
        #     if i not in ds_list:
        #         if i not in cs_list:
        #             last_data[i] = file_data[i]
        for i in total_list:
            last_data[i] = file_data[i]

        # RS分类
        # 重置ds_set
        for i in last_data:
            rs_set[rs_set_count] = [(i,last_data[i])]
            rs_set_count += 1


        # 如果rs_set数量大于n_cluster*3， 则对 RS 非离群数据点在做Kmean 将其分为RS 和 CS
        new_data_point = {}
        for i in rs_set:
            for j in rs_set[i]:
                new_data_point[j[0]] = j[1]

        if len(new_data_point) >= n_cluster*3:
            cluster_for_else = Kmean(new_data_point,n_cluster*3)

            rs_set = {}
            for i in cluster_for_else:
                if len(cluster_for_else[i])>1:
                    cs_set[cs_set_count] = cluster_for_else[i]
                    cs_set_count += 1
                else:
                    rs_set[rs_set_count] = cluster_for_else[i]
                    rs_set_count += 1


######################################################################################################


        # 得到DS的类心
        # 得到DS的每个dimention的标准差
        ds_summerize = cluster_summarize(ds_set)

        cs_centroid = {}
        cs_standard_deviation = {}
        #  ds_summerize : n, sum, sumsq
        #  ds_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
        #  ds_standard_deviation : {0:[0.345, 0.4567, 0.34567], 1:[]...}
        for i in ds_summerize:
            ds_centroid[i] = [each_item/ds_summerize[i][0] for each_item in ds_summerize[i][1]]

        for i in ds_summerize:
            summerize_each = ds_summerize[i]
            summerize_list = []
            for j in range(0,len(summerize_each[1])):
                a = summerize_each[2][j]/summerize_each[0]
                b = (summerize_each[1][j]/summerize_each[0]) * (summerize_each[1][j]/summerize_each[0])
                if (a-b)>0:
                    summerize_result = math.sqrt(a-b)
                else:
                    summerize_result = 1e-10
                summerize_list.append(summerize_result)
            ds_standard_deviation[i] = summerize_list

        # 得到CS的类心
        # 得到CS的每个dimention的标准差
        cs_summerize = cluster_summarize(cs_set)

        cs_centroid = {}
        cs_standard_deviation = {}
        #  cs_summerize : [n, sum, sumsq]
        #  cs_centroid : {0:[234.324523, 34.4567, 87.34567], 1:[]...}
        #  cs_standard_deviation : {0:[0.345, 0.4567, 0.34567], 1:[]...}
        for i in cs_summerize:
            cs_centroid[i] = [each_item/cs_summerize[i][0] for each_item in cs_summerize[i][1]]

        for i in cs_summerize:
            summerize_each = cs_summerize[i]
            summerize_list = []
            for j in range(0,len(summerize_each[1])):
                a = summerize_each[2][j]/summerize_each[0]
                b = (summerize_each[1][j]/summerize_each[0]) * (summerize_each[1][j]/summerize_each[0])
                if (a-b)>0:
                    summerize_result = math.sqrt(a-b)
                else:
                    summerize_result = 1e-10
                summerize_list.append(summerize_result)
            cs_standard_deviation[i] = summerize_list
        ##使用马氏距离
        # pair_list = []
        # for each_centroid in cs_centroid:
        #
        #     new_cs_centroid = {}
        #     for i in cs_centroid:
        #         if i!= each_centroid:
        #             new_cs_centroid[i] = cs_centroid[i]
        #
        #     new_cs_standard_deviation = {}
        #     for i in cs_standard_deviation:
        #         if i!= each_centroid:
        #             new_cs_standard_deviation[i] = cs_standard_deviation[i]
        #
        #     put_which_cluster = Mahalanobis_Distance(cs_centroid[each_centroid], new_cs_centroid, new_cs_standard_deviation)
        #     print(put_which_cluster)
        #     if put_which_cluster != each_centroid and put_which_cluster != "no":
        #         pair_list.append(sorted([each_centroid,put_which_cluster]))

    nof_point_discard = 0
    for each_N in ds_summerize:
        nof_point_discard += ds_summerize[each_N][0]

    nof_point_compression = 0
    for each_N in cs_summerize:
        nof_point_compression += cs_summerize[each_N][0]

    result_for_intermediate = [index,len(ds_set),nof_point_discard,len(cs_set),nof_point_compression,len(rs_set)]

    f2 = open(out_file2, "a")
    csv_writer = csv.writer(f2)
    csv_writer.writerow(result_for_intermediate)
    f2.close()

    if index == len(next_file_path):
        for each_centroid in cs_centroid:
            put_which_cluster = Mahalanobis_Distance2(cs_centroid[each_centroid], ds_centroid, ds_standard_deviation)
            ds_set[put_which_cluster] = ds_set[put_which_cluster] + cs_set[each_centroid]

        for each_centroid in rs_set:
            point = (rs_set[each_centroid][0])[1]
            put_which_cluster = Mahalanobis_Distance2(point, ds_centroid, ds_standard_deviation)
            ds_set[put_which_cluster] = ds_set[put_which_cluster] + rs_set[each_centroid]

    result_dict = {}
    for i in ds_set:
        for j in ds_set[i]:
            result_dict[j[0]] = i

    with open(out_file1, "w") as f1:
        f1.writelines(json.dumps(result_dict))
        f1.close()


    # #test step
    # for i in ds_set:
    #     print(i,len(ds_set[i]))


end = time.time()
print ("Duration: %s Seconds"%(end-start))




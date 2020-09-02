from pyspark import SparkContext,SparkConf
import sys
import os
import json
import time
from pyspark.sql import SparkSession,Row

def get_pair(x,user_business_dict):
    pair_list = []
    for i in user_business_dict:
        if i != x[0]:
            item1 = set(x[1])
            item2 = set(user_business_dict[i])
            length = len(item1&item2)
            if length>=int(filter_threshold):
                pair_list.append(tuple(sorted([x[0],i])))
    return pair_list

def BFS_and_credict(root, edge_dict):

    parent = {}      #node,parent node
    visited = {root:1}   #node,level
    discover = {root:1}

    for single_vertex in edge_dict:
        parent[single_vertex] = []
        if single_vertex != root:
            discover[single_vertex] = "no"

    level = 1    #level count
    level_content = [root]
    while level_content != []:
        new_level_content = []
        for current in level_content:
            visited[current] = level
            all_child_node = edge_dict[current]
            for one_child in all_child_node:
                if discover[one_child] == "no":
                    discover[one_child] = level
                    new_level_content.append(one_child)
                    parent[one_child].append(current)
                elif discover[one_child] == level:
                    parent[one_child].append(current)
        #next level nodes
        level_content = new_level_content
        #next level number
        level = level + 1


    #caculate shortest path number
    sorted_tree = sorted(visited.items(), key= lambda s: s[1])
    sorted_dict = {}
    for i in sorted_tree:
        if i[1] in  sorted_dict:
            sorted_dict[i[1]].append(i[0])
        else:
            sorted_dict[i[1]]=[i[0]]

    count_short_path = {root:1}
    for i in range(2,len(sorted_dict)+1):
        for j in sorted_dict[i]:
            count_short_path[j] = sum([count_short_path[x] for x in parent[j]])


    #caculate betweeness
    edge_value = {}
    node_value = {}
    visited_list = []
    for i in visited:
        visited_list.append(i)

    for i in visited_list:
        node_value[i] = 1
    reverse_visited = visited_list[::-1]

    for child in reverse_visited:
        for parent_node in parent[child]:
            ratio = count_short_path[parent_node]/count_short_path[child]
            credit = node_value[child] * ratio
            edge = tuple(sorted([child,parent_node]))
            if edge in edge_value:
                edge_value[edge] = edge_value[edge]+credit
            else:
                edge_value[edge] = credit
            node_value[parent_node] = node_value[parent_node] + credit

    return edge_value # {edge:betweeness}



def saperate_graph(vertex_list,edge_dict):
    all_community = []
    content = vertex_list
    while content != []:
        community_content = []
        root = content[0]
        queue = [root]
        while queue!=[]:
            i = queue.pop(0)
            community_content.append(i)
            if i in edge_dict:
                for j in edge_dict[i]:
                    if j not in community_content:
                        if j not in queue:
                            queue.append(j)

        all_community.append(community_content)

        content1 = set(content)
        community_content1 = set(community_content)
        content = list(content1-community_content1)

    return all_community



def calculate_modularity(all_community,adjacent_matrix,m,degree_dict):
    sum = 0
    for each in all_community:
        for i in each:
            for j in each:
                cluster_count = adjacent_matrix[(i,j)] - (degree_dict[i]*degree_dict[j] / (2*m))
                sum = sum + cluster_count
    result = sum / (2*m)
    return result







filter_threshold = sys.argv[1]
input_file_path = sys.argv[2]
betweeness_output_file_path = sys.argv[3]
coummunity_output_file_path = sys.argv[4]

# filter_threshold = 7
# input_file_path = "ub_sample_data.csv"
# betweeness_output_file_path = "task2output.txt"
# coummunity_output_file_path = "task2final.txt"

start = time.time()
sc = SparkContext(appName="inf553")
sparkSession = SparkSession(sc)
sc.setLogLevel("WARN")

file_raw = sc.textFile(input_file_path)
first_row = file_raw.first()
all_file = file_raw.filter(lambda s: s != first_row)
user_business_file = all_file.map(lambda s: s.split(",")).map(lambda s: (s[0],s[1]))
user_businesslist_file = user_business_file.groupByKey().map(lambda s: (s[0],list(s[1]))).persist()

user_business_dict = user_businesslist_file.collectAsMap()
user_pair = user_businesslist_file.flatMap(lambda s: get_pair(s,user_business_dict)).distinct().collect()


edge_list = []
vertex_list = []
for each in user_pair:
    edge_list.append(tuple([each[0],each[1]]))
    edge_list.append(tuple([each[1], each[0]]))
    vertex_list.append(each[0])
    vertex_list.append(each[1])
vertex_list = list(set(vertex_list))

# use it as graph to calculate betweeness
edge_dict = {}
for i in edge_list:
    if i[0] in edge_dict:
        edge_dict[i[0]].append(i[1])
    else:
        edge_dict[i[0]] = [i[1]]

betweeness_dict = {}
for each_vertex in vertex_list:
    betweeness_for_one = BFS_and_credict(each_vertex,edge_dict)
    for i in betweeness_for_one:
        if i in betweeness_dict:
            betweeness_dict[i] = betweeness_dict[i]+betweeness_for_one[i]/2
        else:
            betweeness_dict[i] = betweeness_for_one[i]/2

betweeness_result = sorted(betweeness_dict.items(), key= lambda s: s[1],reverse=True)
with open(betweeness_output_file_path, 'w') as output_file:
    for i in betweeness_result:
        output_file.writelines(str(i[0]) + "," + str(i[1]) + "\n")
    output_file.close()




# matrix A
adjacent_matrix = {}
for i in vertex_list:
    for j in vertex_list:
        pair = tuple(sorted([i,j]))
        if pair in betweeness_dict:
            adjacent_matrix[tuple([i,j])] = 1
        else:
            adjacent_matrix[tuple([i, j])] = 0

#degree dict,  vertex: degree
degree_dict = {}
for i in edge_dict:
    degree_dict[i] = len(edge_dict[i])

# number of m
m = len(betweeness_dict)

max_modularity_Q = -1  # Q in range(1,-1)
modularity_dict = {}

while len(betweeness_dict) != 0:
    all_community = saperate_graph(vertex_list,edge_dict)
    modularity = calculate_modularity(all_community,adjacent_matrix,m,degree_dict)
    # modularity_dict[modularity] = all_community
    modularity_dict[modularity] = all_community

    betweeness_dict = {}
    for each_vertex in vertex_list:
        betweeness_for_one = BFS_and_credict(each_vertex, edge_dict)
        for i in betweeness_for_one:
            if i in betweeness_dict:
                betweeness_dict[i] = betweeness_dict[i] + betweeness_for_one[i] / 2
            else:
                betweeness_dict[i] = betweeness_for_one[i] / 2

    if len(betweeness_dict) != 0:
        betweeness_result = sorted(betweeness_dict.items(), key=lambda s: s[1], reverse=True)
        max_betweeness = betweeness_result[0][1]
        remove_list = []
        for i in betweeness_result:
            if i[1] == max_betweeness:
                remove_list.append(i[0])

        for i in remove_list:
            edge_dict[i[0]].remove(i[1])
            edge_dict[i[1]].remove(i[0])





sorted_community = sorted(modularity_dict.items(), key=lambda s: s[0], reverse=True)
community_result = sorted_community[0][1]
result = sorted(community_result,key=lambda s:(len(s),min(s)))

with open(coummunity_output_file_path, 'w') as output_file:
    for i in result:
        output_file.writelines(str(list(sorted(i)))[1:-1] + "\n")
    output_file.close()




end = time.time()
print ("Duration: %s Seconds"%(end-start))

import geopandas as gpd
import pandas as pd
import matplotlib
from shapely.geometry import Point, LineString
from collections import defaultdict
import geopy.distance

feeder = pd.DataFrame(columns=['A', 'B', 'C'])
lower_limit = 0
upper_limit = 0
spanIdCol = 0
leafNodes = []
thresh = 0
nodeParent = {}
pointMap = {}
indMap = {}
spanIdMap = {}
spanLenMap = {}
belowLen = {}
tree = defaultdict(list)
forestRoots = []
numberedMap = defaultdict(int)
color_list = defaultdict(list)
seg_len = defaultdict(int)


def find_feeder(filePath, columnName, feederName, spanIdColName, lower, upper):

    # import ipdb; ipdb.set_trace()

    global feeder
    global spanIdCol
    global lower_limit
    global upper_limit
    lower_limit = lower
    upper_limit = upper
    spanIdCol = spanIdColName
    feeder.iloc[0:0]
    feeder_all = filePath
    feeder = feeder_all.loc[feeder_all[columnName] == feederName]

def distance(s1,s2):
    s1 = (s1[1],s1[0])
    s2 = (s2[1],s2[0])
    return geopy.distance.distance(s1,s2).miles

#merge leaf_node and threshold
def find_leaf_nodes():
    print(feeder)
    count = defaultdict(int)
    leafNodes.clear()
    for row in feeder['geometry']:
        count[row.coords[0]] += 1
        count[row.coords[len(row.coords) - 1]] += 1

    for key,val in count.items():
        if val==1:
            leafNodes.append(key)

#.min on dataframe
def find_threshold():
    global thresh
    spanLenMap.clear()
    minima_len = 1
    count = 0
    for row in feeder['geometry']:
        tempLen = 0
        for j in range(len(row.coords) - 1):
            temp = distance(row.coords[0], row.coords[len(row.coords) - 1])
            tempLen += temp
            if temp==0.0:
                continue
            if temp<minima_len:
                minima_len = temp
        
        spanLenMap[(row.coords[0],row.coords[len(row.coords) - 1])] = tempLen
        spanLenMap[(row.coords[len(row.coords) - 1],row.coords[0])] = tempLen
    
    thresh = minima_len
    count = 0
    while True:
        print(thresh)
        count += 1
        thresh = thresh*10
        if thresh>1:
            break

    thresh = int(thresh)
    for i in range(count):
        thresh = thresh/10

def addEdgeGraph(graph,u,v):
    graph[u].append(v)
    graph[v].append(u)

def addEdgeTree(tree,u,v):
    tree[u].append(v)
    nodeParent[v] = u

def BFS(tree, s, graph, visited):
    queue = []
    queue.append(s)
    visited[s] = True

    while queue:
        s = queue.pop(0)
        for i in graph[s]:
            print(str(s) + "----")
            if visited[i] == False:
                print(i)
                queue.append(i)
                addEdgeTree(tree,s,i)
                visited[i] = True

def lowerLenDFS(node, par):
    if len(tree[node])==0:
        belowLen[node] = 0
        return
    
    temp = 0
    for i in tree[node]:
        if i==par:
            continue
        lowerLenDFS(i, node)
        temp += belowLen[i] + spanLenMap[(indMap[node], indMap[i])]
    
    belowLen[node] = temp
    return

                
def make_tree():
    global spanIdMap
    pointMap.clear()
    indMap.clear()
    spanIdMap.clear()
    tree.clear()
    belowLen.clear()
    count = 0

    graph = defaultdict(list)
    
    for ind in feeder.index:
        row = feeder['geometry'][ind]
        point1 = 0
        point2 = 0
        if pointMap.__contains__(row.coords[0])==False:
            pointMap[row.coords[0]] = count
            indMap[count] = row.coords[0]
            point1 = count
            count += 1
        else:
            point1 = pointMap[row.coords[0]]
        if pointMap.__contains__(row.coords[len(row.coords) - 1])==False:
            pointMap[row.coords[len(row.coords) - 1]] = count
            indMap[count] = row.coords[len(row.coords) - 1]
            point2 = count
            count += 1
        else:
            point2 = pointMap[row.coords[len(row.coords) - 1]]
        addEdgeGraph(graph,point1,point2)
#         print((row.coords[0],row.coords[1]))
#         print(spanIdCol)
        spanIdMap[(row.coords[0],row.coords[len(row.coords) - 1])] = feeder[spanIdCol][ind]
        spanIdMap[(row.coords[len(row.coords) - 1],row.coords[0])] = feeder[spanIdCol][ind]

    leftmost = indMap[0][0]
    upmost = indMap[0][1]
    rightmost = indMap[0][0]

    gridIndMap = defaultdict(list)
    gridPointMap = {}

    for i in indMap:
        if indMap[i][0]<leftmost:
            leftmost = indMap[i][0]
        if indMap[i][1]>upmost:
            upmost = indMap[i][1]
        if indMap[i][0]>rightmost:
            rightmost = indMap[i][0]

    #side length tune(not hard code)
    side = 0.01
    col = int((rightmost - leftmost)/side) + 1
    count = 0
    for i in indMap:
        x = indMap[i][0]
        y = indMap[i][1]
        temp = int((x - leftmost)/side) + int(((upmost - y)/side - 1))*col
        gridPointMap[indMap[i]] = temp
        gridIndMap[temp].append(i)
        count += 1

    for i in indMap:
        temp = gridPointMap[indMap[i]]
        print(i)
        blocks = [temp,temp+1,temp-1,temp+col,temp-col,temp-col-1,temp-col+1,temp+col-1,temp+col+1]
        for k in blocks:
            if gridIndMap.__contains__(k):
                for j in gridIndMap[k]:
                    if distance(indMap[i], indMap[j])<thresh:
                        if i==j:
                            continue
                        print(str(thresh) + "----")
                        print(indMap[i], indMap[j])
                        addEdgeGraph(graph,i,j)
                        spanLenMap[(indMap[i],indMap[j])] = 0
                        spanLenMap[(indMap[j],indMap[i])] = 0


    forestRoots.clear()
    visited = [False] * (len(graph) + 1)
    for node in leafNodes:
        if visited[pointMap[node]]==False:
            nodeParent[node] = -1
            forestRoots.append(pointMap[node])
            BFS(tree, pointMap[node], graph, visited)
            lowerLenDFS(pointMap[node], -1)
            
#     for node in forestRoots:
#         lowerLenDFS(node, -1)

# not permanent sorting operation
    for node in tree:
        tree[node] = sorted(tree[node], key = lambda x:belowLen[x])

def algoBFS(tree, s):
    queue = []
    queue.append((s,0))

    while queue:
        s = queue.pop(0)
        for i in tree[s[0]]:
            if numberedMap[(indMap[s[0]], indMap[i])] == 0:
                numberedMap[(indMap[s[0]], indMap[i])] = s[1]

            queue.append((i,numberedMap[(indMap[s[0]], indMap[i])]))

def custom_algo_helper(tree, v, dist, ind):
    if len(tree[v])==0:
        return (dist, ind)

    if len(tree[v])==1:
        dis = spanLenMap[(indMap[v], indMap[tree[v][0]])] 
        dist, ind = custom_algo_helper(tree, tree[v][0], dist, ind)
        if dis+dist>upper_limit:
            dist = dis
            ind += 1
            numberedMap[(indMap[v], indMap[tree[v][0]])] = ind
            print("1 " + str(indMap[v]) + " " + str(indMap[tree[v][0]]) + " " + str(ind) + " " + str(dist))
        else:
            dist += dis

        return (dist, ind)

    options = []
    for node in tree[v]:
        dis = spanLenMap[(indMap[v], indMap[node])] 
        
        #
        
        dist, ind = custom_algo_helper(tree, node, dist, ind)
        if dist + dis>lower_limit:
#         if True:
            numberedMap[(indMap[v], indMap[node])] = ind
            print("2 " + str(indMap[v]) + " " + str(indMap[node]) + " " + str(ind) + " " + str(dist))
            dist = 0
            ind += 1
        else:
            options.append((node, dis + dist))

    dist = 0
    options.sort(key = lambda x: x[1]) 

    for i,d in options:
        numberedMap[(indMap[v], indMap[node])] = ind
        print("3 " + str(indMap[v]) + " " + str(indMap[node]) + " " + str(ind) + " " + str(dist))
        if d+dist>upper_limit:
            dist = 0
            ind += 1
        else:
            dist += d

    dist = 0
    ind += 1

    return (dist, ind)

def custom_algo(tree, ind):
    dist = 0
    ind = 1
    for node in forestRoots:
        dist, ind = custom_algo_helper(tree, node, dist, ind)
        if dist != 0:
            for dnode in tree[node]:
                if numberedMap[(indMap[node], indMap[dnode])]==0:
                    numberedMap[(indMap[node], indMap[dnode])] = ind
        ind += 1

    for node in forestRoots:
        algoBFS(tree, node)

    return

def algo():
    numberedMap.clear()
    ind = 0
    dist = 0

    custom_algo(tree, ind)

def colorIt(former, later):
    if former==later:
        return
    print(str(former) + " --> " + str(later))
    seg_len[later] += seg_len[former]
    seg_len[former] = 0
    #print(numberedMap[color_list[former][0]])
    for i in color_list[former]:
        #print(numberedMap[i])
        numberedMap[i] = later
        #print(numberedMap[i])
        color_list[later].append(i)
    color_list[former].clear()
    return

def postHelper(v, prevColor):
    if len(tree[v])==0:
        return prevColor

    if len(tree[v])==1:
        cur = numberedMap[(indMap[v], indMap[tree[v][0]])]
        nex = postHelper(tree[v][0], cur)

        if nex==cur:
            return nex
        else:
            nexLen = seg_len[nex]
            if seg_len[cur] + nexLen < upper_limit:
                colorIt(nex, cur)
            return cur

    options = []
    for i in tree[v]:
        cur = numberedMap[(indMap[v], indMap[i])]
        nex = postHelper(i, cur)
        if prevColor== cur:
            continue
        options.append((nex, seg_len[nex]))

    options.sort(key = lambda x: x[1])
    temp = options[0][1]
    color = options[0][0]
    #print(indMap[v])
    #print(options)
    for i in range(1,len(options)):
        if temp + options[i][1]>upper_limit:
            temp = options[i][1]
            color = options[i][0]
        else:
            temp += options[i][1]
            colorIt(options[i][0], color)

    if seg_len[prevColor] + temp<upper_limit:
        colorIt(color, prevColor)
        color = prevColor
    #else:
    #    color = prevColor
    return color

def postAlgo():
    for node in forestRoots:
        trashColor = postHelper(node, -1)
    return

def postPostAlgo():
    for node in forestRoots:
        queue = []
        queue.append((node,-1))
        while queue:
            s = queue.pop(0)
            print(str(s[1]) + "---")
            lessColor = []
            moreColor = []
            if s[1]!=-1 and seg_len[s[1]]>=lower_limit:
                moreColor.append(s[1])
            
            for i in tree[s[0]]:
                color = numberedMap[(indMap[s[0]], indMap[i])]
                #print(color)
                if color==-1:
                    continue
#                 if color==201:
#                     print(str(color) + "--->" + str(seg_len[color]) + "--->" + str(seg_len[color]<1))
                if seg_len[color]<lower_limit and seg_len[color]!=0:
                    lessColor.append(color)
                elif seg_len!=0:
                    moreColor.append(color)
                
            sorted(lessColor, key = lambda x:seg_len[x], reverse=True)
            print(lessColor)
            print(moreColor)
#             if len(lessColor)!=0 and lessColor[0]==339:
#                 print(moreColor)
            if len(moreColor)!=0:
                for i in lessColor:
                    sorted(moreColor, key = lambda x:seg_len[x])
                    print(1)
                    colorIt(i, moreColor[0])
                if len(lessColor)>=2 and lessColor[0]==lessColor[1]:
                    colorIt(lessColor[0], s[1])
            for i in tree[s[0]]:
                color = numberedMap[(indMap[s[0]], indMap[i])]
                if color==-1:
                    continue
                queue.append((i, color))
                
def post_process():
    dataF = pd.DataFrame()
    numpyArr = []
    for i in numberedMap:
        if i== 3: 
            continue
        numpyArr.append(numberedMap[i])

    dataF["id"] = numpyArr
    geometry = []

    for i in numberedMap:
        if i== 3: 
            continue
        geometry.append(LineString([Point(i[0]), Point(i[1])]))

    crs = {'init':'epsg:4326'}
    geodataF = gpd.GeoDataFrame(dataF, crs=crs, geometry = geometry)
    
    seg_len.clear()
    for i in range(len(geodataF)):
        seg_len[geodataF['id'][i]] += spanLenMap[(geodataF['geometry'][i].coords[0], geodataF['geometry'][i].coords[1])]
    
    color_list.clear()
    for i in numberedMap:
        color_list[numberedMap[i]].append(i)

    postAlgo()
    postPostAlgo()
    
def produce_result(resultShpName):
    dataF = pd.DataFrame()
    numpyArr = []
    for i in numberedMap:
        if i== 3: 
            continue
        numpyArr.append(numberedMap[i])

    dataF["id"] = numpyArr
    spanId = []
    #geometry = []
    
    
    for i in numberedMap:
        if i== 3: 
            continue
        if spanIdMap.__contains__((i[0], i[1])):
            spanId.append(spanIdMap[(i[0], i[1])])
        else:
            spanId.append(-1)
        #geometry.append(LineString([Point(i[0]), Point(i[1])]))
        
    dataF[spanIdCol] = spanId  

    crs = {'init':'epsg:4326'}
    #geodataF = gpd.GeoDataFrame(dataF, crs=crs, geometry = geometry)

    final_df = feeder.merge(dataF, on=spanIdCol, how='left')
    geometry = final_df["geometry"].tolist()
    final_df.drop(['geometry'], axis = 1)

    geodataF = gpd.GeoDataFrame(final_df, crs=crs, geometry = geometry)
    
    geodataF_pseudo = geodataF
    geodataF_pseudo["id"].fillna(-1, inplace = True) 
    geodataF_pseudo_p = geodataF_pseudo.loc[geodataF['id'] == -1]

    count = 0
    for i in geodataF_pseudo_p.index:
        p1 = geodataF_pseudo_p['geometry'][i].coords[0]
        p2 = geodataF_pseudo_p['geometry'][i].coords[len(geodataF_pseudo_p['geometry'][i].coords) - 1]
        tempLen = 0
        for j in range(len(geodataF_pseudo_p['geometry'][i].coords) - 1):
            tempLen += distance(geodataF_pseudo_p['geometry'][i].coords[j], geodataF_pseudo_p['geometry'][i].coords[j+1])
        par1 = nodeParent[pointMap[p1]]
        par2 = nodeParent[pointMap[p2]]
        if par1!=-1:
            geodataF_pseudo_p['id'][i] = numberedMap[(indMap[par1], p1)]
        else:
            geodataF_pseudo_p['id'][i] = numberedMap[(indMap[par2], p2)]
        count += 1

    for i in geodataF_pseudo_p.index:
        geodataF_pseudo['id'][geodataF_pseudo[spanIdCol]==geodataF_pseudo_p[spanIdCol][i]] = geodataF_pseudo_p['id'][i]
        #geodataF_pseudo['length_span'][geodataF_pseudo['OBJECTID']==geodataF_pseudo_p['OBJECTID'][i]] = geodataF_pseudo_p['length_span'][i]

    geodataF = geodataF_pseudo
    return geodataF
    # geodataF.to_file("results/" + resultShpName + ".shp")

def produce_result_df():
    dataF = pd.DataFrame()
    numpyArr = []
    for i in numberedMap:
        if i== 3: 
            continue
        numpyArr.append(numberedMap[i])

    dataF["id"] = numpyArr
    spanId = []
    lengthSpan = []

    for i in numberedMap:
        if i== 3: 
            continue
        if spanIdMap.__contains__((i[0], i[1])):
            spanId.append(spanIdMap[(i[0], i[1])])
        else:
            spanId.append(-1)
        lengthSpan.append(spanLenMap[(i[0], i[1])])

    dataF[spanIdCol] = spanId  
    dataF["length_span"] = lengthSpan

    crs = {'init':'epsg:4326'}
    #geodataF = gpd.GeoDataFrame(dataF, crs=crs, geometry = geometry)

    final_df = feeder.merge(dataF, on=spanIdCol, how='left')
    geometry = final_df["geometry"].tolist()
    final_df.drop(['geometry'], axis = 1)

    geodataF = gpd.GeoDataFrame(final_df, crs=crs, geometry = geometry)
    
    geodataF_pseudo = geodataF
    geodataF_pseudo["id"].fillna(-1, inplace = True) 
    geodataF_pseudo_p = geodataF_pseudo.loc[geodataF['id'] == -1]

    count = 0
    for i in geodataF_pseudo_p.index:
        p1 = geodataF_pseudo_p['geometry'][i].coords[0]
        p2 = geodataF_pseudo_p['geometry'][i].coords[len(geodataF_pseudo_p['geometry'][i].coords) - 1]
        tempLen = 0
        for j in range(len(geodataF_pseudo_p['geometry'][i].coords) - 1):
            tempLen += distance(geodataF_pseudo_p['geometry'][i].coords[j], geodataF_pseudo_p['geometry'][i].coords[j+1])
        par1 = nodeParent[pointMap[p1]]
        par2 = nodeParent[pointMap[p2]]
        if par1!=-1:
            geodataF_pseudo_p['id'][i] = numberedMap[(indMap[par1], p1)]
        else:
            geodataF_pseudo_p['id'][i] = numberedMap[(indMap[par2], p2)]
        count += 1

    for i in geodataF_pseudo_p.index:
        geodataF_pseudo['id'][geodataF_pseudo['OBJECTID']==geodataF_pseudo_p['OBJECTID'][i]] = geodataF_pseudo_p['id'][i]
        #geodataF_pseudo['length_span'][geodataF_pseudo['OBJECTID']==geodataF_pseudo_p['OBJECTID'][i]] = geodataF_pseudo_p['length_span'][i]

    geodataF = geodataF_pseudo
    
    return geodataF

def run_the_segmentation_on_feeder(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit, companyName):
    find_feeder(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit)
    feeder = feeder.drop_duplicates(subset = ["feeder_wit"])

    find_leaf_nodes()
    find_threshold()
    make_tree()

    algo()
    post_process()
    return produce_result((companyName + "_" + feederName))

def run_the_segmentation(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit, companyName):

    # import ipdb; ipdb.set_trace()

    global feeder

    feeders = set([])
    feeder_all = path

    for row in feeder_all[feederCol]:
        feeders.add(row)

    count = 0
    result_df = pd.DataFrame(columns=['A', 'B', 'C'])
    for i in feeders:
        feederName = i
        find_feeder(path, feederCol, feederName, spanIdCol, lowerLimit, upperLimit)
        feeder = feeder.drop_duplicates(subset = ["feeder_with_autoid"])
        if len(feeder)==0:
            print(i)
            continue
        find_leaf_nodes()
        
        find_threshold()
        make_tree()
        algo()
        post_process()
        temp_df = produce_result_df()
        if count==0:
            result_df.iloc[0:0]
            result_df = temp_df
        else:
            result_df = result_df.append(temp_df)
        count += 1
            
    geometry = result_df["geometry"].tolist()
    result_df.drop(['geometry'], axis = 1)
    crs = {'init':'epsg:4326'}
    geodataF = gpd.GeoDataFrame(result_df, crs=crs, geometry = geometry)

    return geodataF
    # geodataF.to_file("results/" + companyName + ".shp")
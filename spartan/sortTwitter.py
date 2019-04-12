###
###
#!python3
import sys
import json
from mpi4py import MPI
import time
import os
import numpy as np

def seperate(path, mpiSize):
    fileEnd=os.path.getsize(path)
    gridSize=fileEnd//mpiSize
    overFlow=fileEnd%mpiSize
    print('fileEnd:',fileEnd)
    print('gridSize:',gridSize)
    partition=[]
    with open(path,'rb') as f:
        gridEnd = f.tell()
        while True:
            if fileEnd-gridEnd<gridSize+overFlow:
                partition.append([gridEnd,fileEnd-gridEnd])
                break
            gridStart=gridEnd
            f.seek(gridSize, 1)
            f.readline()
            gridEnd=f.tell()
            partition.append([gridStart,gridEnd-gridStart])
    return partition

def readTwitter(path,partitionList):
    partition=partitionList[0]
    twitterPost=[]
    with open(path, 'rb') as f:
        f.seek(partition[0])
        while True:
            if f.tell()>=partition[0]+partition[1]:
                break
            line=f.readline().decode('utf-8')
            # print(type(line))
            twitterInfo=dealTwitter(line)
            # print(twitterInfo)
            if twitterInfo:
                twitterPost.append(twitterInfo)
    return twitterPost

def dealTwitter(line):
    twitterInfo=[]
    goodData = False
    line=line.strip()
    if line[0] == '{' and line[-1] == '}' and len(line) > 3:
        goodData = True
    elif line[0] == '{' and line[-2] == '}' and len(line) > 3:
        line = line[:-1]
        goodData = True
    if goodData:
        js2 = json.loads(line)
        if js2['doc']['coordinates'] == None:
            return None
        else:
            twitteCoordinate = js2['doc']['coordinates']['coordinates']
            hashtag = js2['doc']['entities']['hashtags']
            hashtags=[]
            for j in hashtag:
                tag = j['text'].encode('utf8')  # some words are not utf-8 type, will cause can-not-output error
                tag = tag.decode('utf8')  # change tytes type to str
                tag = tag.lower()  # charactor insensitive
                tag = '#' + tag
                hashtags.append(tag)
            twitterInfo=[twitteCoordinate, hashtags, None]
    return twitterInfo

def extractFromGrid(file_path):
    #data structure example:[[id,[coordinates],twitterCount,[[hashtage,count],...],tagSum],...]
    # read melbGrid.json
    with open(file_path, encoding='utf-8',errors='ignore') as f:
        js = json.load(f)
    f.close()
    # extract useful information from melbGrid.json
    listFeatures = js['features']
    #print 'features:', listFeatures
    #region = {'id':{'coordinate':[],'twitterNum':0,'hashtag':[],'tagSum':0}}
    region={}
    for entry in listFeatures:
        property = entry['properties']['id']
        coordinate = entry['geometry']['coordinates'][0]
        region[property.encode('utf8')]={}
        region[property.encode('utf8')]['coordinate']=coordinate
        region[property.encode('utf8')]['twitterNum']=0
        region[property.encode('utf8')]['hashtag']={}
        region[property.encode('utf8')]['tagSum']=0
    #print region
    return region

def countNum(region,twitterPost):
    for entry in twitterPost:
        x=entry[0][0]
        y=entry[0][1]
        hashtags = entry[1]
        if x>=144.7 and x<=145.45 and y>=-38.1 and y<=-37.5:
            if y>=-37.8:
                if y>=-37.65:
                    if x>145:
                        if x>145.15 and x<=145.3:
                            entry[2]='A4'
                        elif x<=145.15:
                            entry[2]='A3'
                    else:
                        if x>144.85:
                            entry[2]='A2'
                        else:
                            entry[2]='A1'
                else:
                    if x>145:
                        if x>145.15 and x<=145.3:
                            entry[2]='B4'
                        elif x<=145.15:
                            entry[2]='B3'
                    else:
                        if x>144.85:
                            entry[2]='B2'
                        else:
                            entry[2]='B1'
            else:
                if y>=-37.95:
                    if x>145:
                        if x>145.15:
                            if x>145.3:
                                entry[2]='C5'
                            else:
                                entry[2]='C4'
                        else:
                            entry[2]='C3'
                    else:
                        if x>144.85:
                            entry[2]='C2'
                        else:
                            entry[2]='C1'
                else:
                    if x>=145:
                        if x>145.15:
                            if x>145.3:
                                entry[2]='D5'
                            else:
                                entry[2]='D4'
                        else:
                            entry[2]='D3'
        for key in region:
            if entry[2]==key.decode('utf8'):
                # print ('twitter id equals region id')
                region[key]['twitterNum']+=1
                for tag in hashtags:
                    if tag in region[key]['hashtag']:
                        region[key]['hashtag'][tag]+=1
                    else:
                        region[key]['hashtag'][tag]=1
    return region

#Order hashtags by numbers for each Grid
def orderHashtags(region):
    tagOrder={}
    for key in region:
        tagdict=region[key]['hashtag']
        tagRank=sorted(tagdict,key=lambda k:tagdict[k],reverse=True)
        tagOrder[key]=tagRank
        # print (tagOrder)
        # region[key]['hashtag']=tagList
    return tagOrder

#the object to be scattered needs to be the same size of processor size
#divide twitter to de same size as size of mpi processors
def divideTwitter(twitterPost, size):
    if size==1:
        return twitterPost
    twitterSize=len(twitterPost)
    intervalLong=twitterSize//size
    twitterDivide=[]
    i=0
    while i<twitterSize:
        if twitterSize-i<2*intervalLong:
            twitterDivide.append(twitterPost[i:])
            break
        else:
            twitterDivide.append(twitterPost[i:i+intervalLong])
            i=i+intervalLong
    return twitterDivide

#rearrange information in region after gathering
def rearrangeRegion(region):
    if len(region)>1:
        #print ("region data needs rearranged.")
        i=1
        while i<len(region):
            for key in region[i]:
                region[0][key]['twitterNum']+=region[i][key]['twitterNum']
                region[0][key]['tagSum']+=region[i][key]['tagSum']
                tags=region[0][key]['hashtag']
                tagAdd=region[i][key]['hashtag']
                for tag in tagAdd:
                    tagHere=False
                    for eachTag in tags:
                        if tag[0]==eachTag[0]:
                            eachTag[1]+=tag[1]
                            tagHere=True
                    if not tagHere:
                        tags.append(tag)
            i+=1
    region=region[0]
    return region

def output(region,regionList,tagOrder):
    print(u'Grid ranking by post decending:')
    for entry in regionList:
        print('%s: %d posts,'%(entry,region[entry]['twitterNum']))
    print()
    print('Top 5 hashtags for each Grid:')
    for entry in regionList:
        # tags=region[entry]['hashtag'][:5]
        tags=tagOrder[entry][:5]
        if len(tags)==0:
            print('%s:'%(entry))
        else:
            print('%s:('%(entry),end='')
            i=0
            while i<len(tags):
                if i==len(tags)-1:
                    print('(%s, %d))'%(tags[i].encode('utf8'),region[entry]['hashtag'][tags[i]]))
                else:
                    print('(%s, %d),'%(tags[i].encode('utf8'),region[entry]['hashtag'][tags[i]]),end='')
                i+=1
start=time.time()
comm=MPI.COMM_WORLD
comm_rank=comm.Get_rank()
comm_size=comm.Get_size()
file_path='melbGrid.json'
file_path2='smallTwitter.json'
region=extractFromGrid(file_path)

if comm_rank==0:
    partitionList=seperate(file_path2,comm_size)
    partition=np.array_split(partitionList, comm_size)
    print (len(partition))
else:
    partition=[]
try:
    partition=comm.scatter(partition,root=0)
except:
    print('scatter goes wrong.')
    sys.exit(0)
start=time.time()
twitterPost=readTwitter(file_path2, partition)
readTime=time.time()-start
region=countNum(region, twitterPost)
try:
    region=comm.gather(region, root=0)
except:
    print('gather goes wrong.')
    sys.exit(0)
if comm_rank==0:
    region=rearrangeRegion(region)
    regionList=sorted(region, key=lambda k:region[k]['twitterNum'],reverse=True)
    tagOrder=orderHashtags(region)
    output(region,regionList,tagOrder)
sys.exit(0)
###
###
#!python3
import sys
import json
from mpi4py import MPI
import time

def extractFromGrid(file_path):
    #data structure example:[[id,[coordinates],twitterCount,[[hashtage,count],...],tagSum],...]
    # read melbGrid.json
    with open(file_path, encoding='utf-8',errors='ignore') as f:
        js = json.load(f)
    f.close()
    # extract useful information from melbGrid.json
    listFeatures = js['features']
    #print 'features:', listFeatures
    region = []
    for entry in listFeatures:
        property = entry['properties']['id']
        coordinate = entry['geometry']['coordinates'][0]
        region.append([property, coordinate,0,[],0])
    #print region
    return region

def extractFromTwitter(file_path):
    #data structure example:[[[coordinates],[hashtages],[areaId]],....]
    # read tinyTwitter.json
    twitterPost=[]
    with open(file_path2,encoding='utf-8') as f2:
        for line in f2:
            if line[0] == '{':
                i = -1
                while i >= -3:
                    if line[i] == '}':
                        line = line[:i + 1]
                        js2 = json.loads(line)
                        if js2['doc']['coordinates']==None:
                            break
                        else:
                            twitteCoordinate = js2['doc']['coordinates']['coordinates']
                            hashtag = js2['doc']['entities']['hashtags']
                            hashtags = []
                            for j in hashtag:
                                tag=j['text'].encode('utf8')#some words are not utf-8 type, will cause can-not-output error
                                tag=tag.decode('utf8')#change tytes type to str
                                tag=tag.lower()#charactor insensitive
                                tag='#'+tag
                                hashtags.append(tag)
                            twitterPost.append([twitteCoordinate, hashtags, None])
                            break
                    i = i - 1
    return twitterPost

#select twitters which are within given Grids, and calculate their numbers
def countPost(region, twitterPost):
    for entry in twitterPost:
        x=entry[0][0]
        y=entry[0][1]
        for area in region:
            upperLeft,upperRight,bottomRight,bottomLeft=area[1][0],area[1][1],area[1][2],area[1][3]
            if area[0]=='A1':
                if x>=upperLeft[0] and x<=upperRight[0] and y>=bottomRight[1] and y<=upperRight[1]:
                    area[2]+=1
                    entry[2]='A1'
            elif area[0]=='A2' or area[0]=='A3' or area[0]=='A4':
                if x>upperLeft[0] and x<=upperRight[0] and y>=bottomRight[1] and y<=upperRight[1]:
                    area[2]+=1
                    entry[2]=area[0]
            elif area[0]=='B1' or area[0]=='C1':
                if x>=upperLeft[0] and x<=upperRight[0] and y>=bottomRight[1] and y<upperRight[1]:
                    area[2]+=1
                    entry[2]=area[0]
            else:
                if x>upperLeft[0] and x<=upperRight[0] and y>=bottomRight[1] and y<upperRight[1]:
                    area[2]+=1
                    entry[2]=area[0]
    return region

def countHashtags(region, twitterPost):
    for entry in twitterPost:
        hashtags=entry[1]
        areaId=entry[2]
        for area in region:
            if areaId==area[0]:
                for tag in hashtags:
                    k=0
                    for hashtag in area[3]:
                        if tag==hashtag[0]:
                            hashtag[1]+=1
                            k=1
                    if k==0:
                        area[3].append([tag,1])
                    area[4]+=1
    return region

#Order Grids by the number of twitter within
def orderPost(region):
    i=0
    while i<len(region):
        max=region[i][2]
        j=i+1
        locate=i
        while j<len(region):
            if region[j][2]>max:
                max=region[j][2]
                locate=j
            j+=1
        if locate!=i:
            temp=region[locate]
            region[locate]=region[i]
            region[i]=temp
        i+=1
    return region

#Order hashtags by numbers for each Grid
def orderHashtags(region):
    for entry in region:
        tagList=entry[3]
        if len(tagList)!=0:
            i=0
            while i<len(tagList):
                max=tagList[i][1]
                j=i+1
                locate=i
                while j<len(tagList):
                    if tagList[j][1]>max:
                        max=tagList[j][1]
                        locate=j
                    j+=1
                if locate!=i:
                    temp=tagList[locate]
                    tagList[locate]=tagList[i]
                    tagList[i]=temp
                i+=1
    return region

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
            entry=0
            while entry<len(region[0]):
                region[0][entry][2]+=region[i][entry][2]
                region[0][entry][4]+=region[i][entry][4]
                tags=region[0][entry][3]
                tagAdd=region[i][entry][3]
                for tag in tagAdd:
                    tagHere=False
                    for eachTag in tags:
                        if tag[0]==eachTag[0]:
                            eachTag[1]+=tag[1]
                            tagHere=True
                    if not tagHere:
                        tags.append(tag)
                entry+=1
            i+=1
        region=region[0]
    return region

def output(region):
    print(u'Grid ranking by post decending:')
    for entry in region:
        print('%s: %d posts,'%(entry[0],entry[2]))
    print()
    print('Top 5 hashtags for each Grid:')
    for entry in region:
        tags=entry[3][:5]
        if len(tags)==0:
            print('%s:'%(entry[0]))
        else:
            print('%s:('%(entry[0]),end='')
            i=0
            while i<len(tags):
                if i==len(tags)-1:
                    print('(%s, %d))'%(tags[i][0],tags[i][1]))
                else:
                    print('(%s, %d),'%(tags[i][0],tags[i][1]),end='')
                i+=1
start=time.time()
comm=MPI.COMM_WORLD
comm_rank=comm.Get_rank()
comm_size=comm.Get_size()
file_path='melbGrid.json'
file_path2='bigTwitter.json'
region=extractFromGrid(file_path)
if comm_size==1:
    twitterPost = extractFromTwitter(file_path2)
    readTime=time.time()-start
    print (readTime)
    region=countPost(region, twitterPost)
    region=countHashtags(region, twitterPost)
    region = orderPost(region)
    region = orderHashtags(region)
    output(region)
else:
    if comm_rank==0:
        twitterPost=extractFromTwitter(file_path2)
        twitterData=divideTwitter(twitterPost, comm_size)
    else:
        twitterData=[]
    try:
        twitterData=comm.scatter(twitterData, root=0)
    except:
        print('scatter goes wrong.')
        sys.exit(0)
    region=countPost(region, twitterData)
    region=countHashtags(region,twitterData)
    try:
        region=comm.gather(region, root=0)
    except:
        print('gather goes wrong.')
        sys.exit(0)
    if comm_rank==0:
        region=rearrangeRegion(region)
        # print (len(region))
        region=orderPost(region)
        region=orderHashtags(region)
        output(region)
sys.exit(0)
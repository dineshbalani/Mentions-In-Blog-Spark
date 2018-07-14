import os
import sys
import re

#Done to configure spark. Remove if you are able to import SparkContext from pyspark
def configure_spark(spark_home=None, pyspark_python=None):
    spark_home = spark_home
    os.environ['SPARK_HOME'] = spark_home

    # Add the PySpark directories to the Python path:
    sys.path.insert(1, os.path.join(spark_home, 'python'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'pyspark'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'build'))

    # If PySpark isn't specified, use currently running Python binary:
    pyspark_python = pyspark_python or sys.executable
    os.environ['PYSPARK_PYTHON'] = pyspark_python

configure_spark('C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7')
#os.environ['SPARK_HOME'] = 'C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7'
#sys.path.append('C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7bin')

from pyspark import SparkContext

#splits and gets the industry names from the files
def getIndustryName(FileName):
    return FileName.split('/')[-1].split('.')[-3]

#returns tuple of (date,post)
def returnDatePosts(content):
    pattern = re.compile('<date>\s*(.*)\s*</date>\s*<post>\s*(.*)\s*</post>', re.MULTILINE)
    instances = pattern.findall(content)
    return instances

#checks for industry names in each post and returns tuple of (industry,date.coount)
def getIndCount(date,post,fileNames):
    IndCount =[]
    Indtuple =()
    finalDate = date[-4:]+'-'+date[3:-4]
    for fileName in fileNames:
        occ=[]
        pattern = re.compile(fileName, re.IGNORECASE)
        instances = pattern.findall(post)
        # print(instances)
        if(len(instances)!=0):
            occ.append(instances)
            Indtemp = (fileName)
            Indtuple=(finalDate, len(instances))
            Indtemp = (Indtemp, Indtuple)
            IndCount.append(Indtemp)
    if(len(IndCount)!=0):
        return IndCount


sc=SparkContext.getOrCreate()
dataDir =r'C:\Users\SSDN-Dinesh\Desktop\SBU\BDA\sblogs'
data = sc.wholeTextFiles(dataDir)

#Getting all the filenames and storing in broadcase variable
fileName = data.map(lambda x:((getIndustryName(x[0]).lower()),1)).reduceByKey(lambda x,y:x+y)
fileNames = fileName.keys().collect()
broadCastFiles = sc.broadcast(fileNames)

#Getting map of date and posts in a blog and transforming them to the format of(industry,date,counts)
getContent = data.flatMap(lambda x:returnDatePosts(x[1])).map(lambda x:(getIndCount(x[0],x[1],fileNames))).filter(lambda x: x is not None)
#first reducing((industry,month),count) by key and then reducing (industry,(month,count)) by key
fileContents = getContent.flatMap(lambda x:x).map(lambda x:((x[0],x[1][0]),x[1][1])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0][0],[(x[0][1],x[1])])).reduceByKey(lambda x,y:(x+y)).collect()

print(fileContents)

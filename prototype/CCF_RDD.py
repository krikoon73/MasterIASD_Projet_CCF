import findspark
findspark.init()
from pyspark.sql import SparkSession 
from pyspark import SparkConf,SparkContext
import os

conf = SparkConf()
conf.setMaster('local')
conf.setAppName('pyspark-shell-CCF')
conf.set('spark.driver.host', '127.0.0.1')
conf.set("spark.ui.proxyBase", "")
conf.set("spark.cores.max","4")
conf.set("spark.executor.cores","2")
os.environ['PYSPARK_PYTHON'] = '/Users/ccompain/.pyenv/versions/miniconda3-latest/bin/python' # Needs to be explicitly provided as env. Otherwise workers run Python 2.7
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter' 
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

def CCF_dedup(data):
    dedup = data.map(lambda x: ((x[0],x[1]), 1))\
        .reduceByKey(lambda x,y: 1)\
        .map(lambda x: (x[0][0], x[0][1]))
        #.sortBy(lambda x: x[0])
    return dedup

def CCF_Iterate_map(pair):
    #map
    pair2=pair.map(lambda x: (x[1], x[0]))
    map_pair=pair.union(pair2)
    return map_pair

def f(x): return x
    
def CCF_Iterate_reduce(data):
    #find min value per key
    #couple (key, min)
    key_min=data.reduceByKey(lambda x,y: min(x,y)).filter(lambda x: x[0]>x[1])
    #put together: in one command
    valuelist_min=key_min.join(data).map(lambda x: (x[0],x[1][1])).cogroup(key_min).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
    
    #make RDD : (min, (list of value))
    min_valuelist=valuelist_min.map(lambda x:(x[1][1][0], x[1][0]))

    #use flatmapvalue to transform to RDD (min, value), then filter min != value, map to couple (value, min)

    min_value=min_valuelist.flatMapValues(f).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[1],x[0]))
    countnewpair=min_value.count()

    #union couple (key, min) and (value, min)
    #unionkeyminvalue=key_min.union(min_value).sortBy(lambda x:x[1])
    unionkeyminvalue=key_min.union(min_value)

    return unionkeyminvalue

r=sc.parallelize([("A","B"),("B","C"),("B","D"),("D","E"),("F","G"),("G","H")])
#r.collect()

new_pair_flag = True
iteration = 0
new_reduce = r
current_size = new_reduce.count()

while new_pair_flag:
    iteration +=1
    print(f"*** Iteration {iteration} ***")
    new_map = CCF_Iterate_map(new_reduce)
    new_reduce_tmp = CCF_Iterate_reduce(new_map)
    new_size = new_reduce_tmp.count()
    if (new_size-current_size)!=0:
        new_reduce = CCF_dedup(new_reduce_tmp)
    else:
        new_reduce = CCF_dedup(new_reduce_tmp)
        new_reduce.coalesce(1).saveAsTextFile("hdfs:/CCF/output")
        new_pair_flag = False
print("################################")
print("Number of iterations :",iteration)
print("################################")

    

'''
map1= CCF_Iterate_map(r)
reduce1_tmp=CCF_Iterate_reduce(map1)
reduce1=CCF_dedup(reduce1_tmp)
#reduce1.coalesce(1).saveAsTextFile("hdfs:/CCF/output")

map2 = CCF_Iterate_map(reduce1)
reduce2_tmp=CCF_Iterate_reduce(map2)
reduce2=CCF_dedup(reduce2_tmp)
#reduce2.coalesce(1).saveAsTextFile("hdfs:/CCF/output")

map3 = CCF_Iterate_map(reduce2)
reduce3_tmp=CCF_Iterate_reduce(map3)
reduce3=CCF_dedup(reduce3_tmp)
#reduce3.coalesce(1).saveAsTextFile("hdfs:/CCF/output")

map4 = CCF_Iterate_map(reduce3)
reduce4_tmp=CCF_Iterate_reduce(map4)
reduce4=CCF_dedup(reduce4_tmp)

reduce4.coalesce(1).saveAsTextFile("hdfs:/CCF/output")
'''
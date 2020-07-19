import findspark
findspark.init()
#from pyspark.sql import SparkSession 
from pyspark import SparkConf,SparkContext
import os
import subprocess

# Initialize spark-context configuration
conf = SparkConf()
conf.setMaster('local')
conf.setAppName('pyspark-shell-CCF')
# Local execution
conf.set('spark.driver.host', '127.0.0.1')
# Just for having a nice gui locally
conf.set("spark.ui.proxyBase", "")
# Local exec paramters
conf.set("spark.cores.max","4")
conf.set("spark.executor.cores","2")
# Needs to be explicitly provided as env. Otherwise workers run Python 2.7
os.environ['PYSPARK_PYTHON'] = '/Users/ccompain/.pyenv/versions/miniconda3-latest/bin/python' 
# For jupyter notebook => os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter' 
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
# Build the spark-context 
sc = SparkContext(conf=conf)
# Set logging level to avoid [INFO] message at the console
sc.setLogLevel("WARN")

def CCF_dedup(data):
    dedup = data.map(lambda x: ((x[0],x[1]), 1))\
        .reduceByKey(lambda x,y: 1)\
        .map(lambda x: (x[0][0], x[0][1]))
        #.sortBy(lambda x: x[0])
    return dedup

def CCF_Iterate_map(pair):
    # map
    pair2=pair.map(lambda x: (x[1], x[0]))
    map_pair=pair.union(pair2)
    return map_pair

def f(x): return x
    
def CCF_Iterate_reduce(data):
    # find min value per key
    # couple (key, min)
    key_min=data.reduceByKey(lambda x,y: min(x,y)).filter(lambda x: x[0]>x[1])
    # put together: in one command
    valuelist_min=key_min.join(data).map(lambda x: (x[0],x[1][1])).cogroup(key_min).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
    
    # Build RDD : (min, (list of value))
    min_valuelist=valuelist_min.map(lambda x:(x[1][1][0], x[1][0]))

    # Use flatmapvalue to transform to RDD (min, value), then filter min != value, map to couple (value, min)

    min_value=min_valuelist.flatMapValues(f).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[1],x[0]))
    countnewpair=min_value.count()
    #print(f"countnewpair : {countnewpair}")

    # Union couple (key, min) and (value, min)
    # Sorted => unionkeyminvalue=key_min.union(min_value).sortBy(lambda x:x[1])
    unionkeyminvalue=key_min.union(min_value)

#    return unionkeyminvalue
    return unionkeyminvalue,countnewpair

# Initialise logger
#log4jLogger = sc._jvm.org.apache.log4j
#LOGGER = log4jLogger.LogManager.getLogger(__name__)

# Simple example
# r=sc.parallelize([("A","B"),("B","C"),("B","D"),("D","E"),("F","G"),("G","H")])

# Import the file as RDD
## Get the local path 
# directory = os.path.abspath(os.getcwd())
storage = "hdfs:"
input_directory = "/CCF/input"
output_directory = "/CCF/output"
partition_number = 1
## Explicit filename as input data
input_filename = "example.csv"
#input_filename = "simple_random_graph.csv"
#input_filename = "simple_2_graphs.csv"
## Build the absolute file path
#file_path = "file://" + pwd + "/" + filename
input_file_path = storage + input_directory + "/" + input_filename
## Import as RDD line_by_line
raw_graph = sc.textFile(input_file_path,minPartitions=partition_number)
## CSV transformation -> Separator need to be adapted considering the file format
r = raw_graph.map(lambda x:x.split(',')).map(lambda x:(x[0],x[1]))
## Cleaning - Delete previous results
subprocess.call(["hdfs", "dfs", "-rm", "-R", output_directory])

new_pair_flag = True
iteration = 0
new_reduce = r
current_size = new_reduce.count()
number_partition = new_reduce.getNumPartitions()

print("################################")
print(" Start CCF RDD")
print("--------------------------------")
print(f"Number of pairs :{current_size}")
print(f"Number of partitions : {number_partition}")

while new_pair_flag:
    iteration +=1
    print(f"*** Iteration {iteration} ***")
    new_map = CCF_Iterate_map(new_reduce)
    new_reduce_tmp,new_pairs = CCF_Iterate_reduce(new_map)
    print(f"--> Iteration {iteration} : Number of new pairs = {new_pairs}")
    if (new_pairs)>0:
        new_reduce = CCF_dedup(new_reduce_tmp)
    else:
        print("*** Stop the loop ***")
        print("Clean the last RDD of duplicate pairs")
        new_reduce = CCF_dedup(new_reduce_tmp)
        print(f"Save last RDD in {output_directory}")
        new_reduce.coalesce(1).saveAsTextFile(output_directory)
        new_pair_flag = False
print("--------------------------------")
print("Total iterations :",iteration)
print("Results dump : ")
subprocess.call(["hdfs", "dfs", "-ls", output_directory])
print("################################")



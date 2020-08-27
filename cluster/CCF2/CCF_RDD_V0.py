from pyspark import SparkConf,SparkContext
import os
import argparse
from time import time

def CCF_dedup(data):
  dedup = data.map(lambda x: ((x[0],x[1]), 1))\
              .reduceByKey(lambda x,y: 1)\
              .map(lambda x: (x[0][0], x[0][1]))
  return dedup

def CCF_Iterate_map(pair):
    #map
    pair2=pair.map(lambda x: (x[1], x[0]))
    map_pair=pair.union(pair2)
    #reduce partition
    map_pair_c= map_pair.coalesce(partition_number)
    return map_pair_c

def f(x): return x

def CCF_Iterate_reduce(data):
    #find min value per key
    #couple (key, min)
    key_min=data.reduceByKey(lambda x,y: min(x,y)).filter(lambda x: x[0]>x[1])
    #find value list min
    #valuelist_min=key_min.join(data).map(lambda x: (x[0],x[1][1])).cogroup(key_min).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
    valuelist_min_join=key_min.join(data)
    #reduce partition
    valuelist_min_c1= valuelist_min_join.coalesce(partition_number)
    valuelist_min_c=valuelist_min_c1.map(lambda x: (x[0],x[1][1])).cogroup(key_min).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
    #make RDD : (min, (list of value))
    min_valuelist=valuelist_min_c.map(lambda x:(x[1][1][0], x[1][0]))

    #use flatmapvalue to transform to RDD (min, value), then filter min != value, map to couple (value, min)

    min_value=min_valuelist.flatMapValues(f).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[1],x[0]))
    countnewpair=min_value.count()

    #union couple (key, min) and (value, min)
    unionkeyminvalue=key_min.union(min_value)
    #reduce partition
    unionkeyminvalue_c=unionkeyminvalue.coalesce(partition_number)
    return unionkeyminvalue_c, countnewpair


# Inialize parser and parse argument
parser = argparse.ArgumentParser()
parser.add_argument("-input","--input",help="Complete input file path for Dataset ex. hdfs:/CCF/input/example.csv")
parser.add_argument("-output","--output",help="Complete output path for results ex. hdfs:/CCF/output")
parser.add_argument("-partition","--partition",type=int,help="Number of partitions for dataset")
args = parser.parse_args()
partition_number = args.partition
input_file_path = args.input
output_directory = args.output

# Initialize spark-context configuration
conf = SparkConf()
conf.setAppName('pyspark-shell-CCF-v1')

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Initialize logger
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

LOGGER.warn("################################")
LOGGER.warn(" Start CCF RDD version 1")
LOGGER.warn("--------------------------------")

# Import as RDD line_by_line
raw_graph = sc.textFile(input_file_path,minPartitions=partition_number)

# CSV transformation -> Separator need to be adapted considering the file format
r = raw_graph.map(lambda x:x.split('\t')).map(lambda x:(x[0],x[1]))

new_pair_flag = True
iteration = 0
#accum = sc.accumulator(0)
graph = r
current_size = graph.count()
number_partition = graph.getNumPartitions()

LOGGER.warn("Input dataset : "+input_file_path)
LOGGER.warn("Number of pairs = "+str(current_size))
LOGGER.warn("Number of partitions = "+str(number_partition))

start_time = time()

while new_pair_flag:
    iteration += 1
    #accum.value = 0

    # CCF-iterate (MAP)
    new_map = CCF_Iterate_map(graph)


    # CCF-iterate (REDUCE)
    #reduceJob = mapJob.groupByKey().flatMap(lambda pair: CCF_Iterate_reduce(pair)).sortByKey()
    new_reduce_tmp,new_pairs = CCF_Iterate_reduce(new_map)

    if (new_pairs)>0:
        graph = CCF_dedup(new_reduce_tmp)
        #print("number of partitions after reduce")
        #print(new_reduce.glom().getNumPartitions())
    else:
        graph = CCF_dedup(new_reduce_tmp)
        #print("number of partitions after reduce")
        #print(new_reduce.glom().getNumPartitions())

        new_pair_flag = False
    LOGGER.warn("Iteration "+str(iteration)+" ===> "+"newPairs = "+str(new_pairs))

process_time_checkpoint = time()
LOGGER.warn("Number of connected components = "+str(graph.count()))
process_time = process_time_checkpoint - start_time
LOGGER.warn("Process time = "+str(process_time))
LOGGER.warn("Save last RDD in "+output_directory)
graph.coalesce(1).saveAsTextFile(output_directory)
write_time_checkpoint = time()
write_time = write_time_checkpoint - process_time_checkpoint
LOGGER.warn("RDD write time = "+str(write_time))

# Optional : analysis of results
#results = list(map(lambda e: e[::-1], graph.collect()))
results = list(graph.map(lambda e: e[::-1]).groupByKey().map(lambda x : (x[0],tuple(x[1]))).collect())
for k in results:
    LOGGER.warn("Component id: "+str(k[0])+"| Number of nodes= "+str(len(k[1])+1))
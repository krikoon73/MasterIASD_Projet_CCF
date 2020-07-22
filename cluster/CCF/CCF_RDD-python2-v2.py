from pyspark import SparkConf,SparkContext
import os
import argparse
from time import time

def CCF_Iterate_reduce(pair):
    #accum
    key, values = pair
    min = key
    valueL = []
    for value in values:
        if value < min:
            min = value
        valueL.append(value)
    if min < key:
        yield((key, min))
        for value in valueL:
            if min != value:
                accum.add(1)
                yield((value, min))

def CCF_Iterate_reduce_SS(pair):
  key, values = pair[0],list(pair[1])
  global accum
  values.sort()
  min = values.pop(0)
  if min < key:
    yield((key, min))
    for value in values:
      accum.add(1)
      yield((value, min))

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
conf.setAppName('pyspark-shell-CCF-v2')

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN") 

# Initialize logger
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

LOGGER.warn("################################")
LOGGER.warn(" Start CCF RDD ")
LOGGER.warn("--------------------------------")

# Import as RDD line_by_line
raw_graph = sc.textFile(input_file_path,minPartitions=partition_number)

# CSV transformation -> Separator need to be adapted considering the file format
r = raw_graph.map(lambda x:x.split(',')).map(lambda x:(x[0],x[1]))

new_pair_flag = True
iteration = 0
accum = sc.accumulator(0)
graph = r
current_size = graph.count()
number_partition = graph.getNumPartitions()

LOGGER.warn("Input dataset : "+input_file_path)
LOGGER.warn("Number of pairs = "+str(current_size))
LOGGER.warn("Number of partitions = "+str(number_partition))

start_time = time()

while new_pair_flag:
    iteration += 1
    newPair = False
    accum.value = 0

    # CCF-iterate (MAP)
    mapJob = graph.flatMap(lambda e: (e,e[::-1]))

    # CCF-iterate (REDUCE)
    #reduceJob = mapJob.groupByKey().flatMap(lambda pair: CCF_Iterate_reduce(pair)).sortByKey()
    reduceJob = mapJob.groupByKey().flatMap(lambda pair: CCF_Iterate_reduce_SS(pair)).sortByKey()
    #print(reduceJob.collect())

    # CCF-dedup 
    dedupJob = reduceJob.distinct()
    dedupJob.persist()

    # Force the RDD evalusation
    # tmp = dedupJob.count()

    # Prepare next iteration
    graph = dedupJob
    new_pair_number = accum.value
    new_pair_flag = bool(new_pair_number)

    LOGGER.warn("Iteration "+str(iteration)+" ===> "+"newPairs = "+str(new_pair_number))

process_time_checkpoint = time()
tmp = dedupJob.count()
LOGGER.warn("Number of connected components = "+str(tmp))
process_time = process_time_checkpoint - start_time
LOGGER.warn("Process time = "+str(process_time))
LOGGER.warn("Save last RDD in "+output_directory)
graph.coalesce(1).saveAsTextFile(output_directory)
write_time_checkpoint = time()
write_time = write_time_checkpoint - process_time_checkpoint
LOGGER.warn("RDD write time = "+str(write_time)) 

# Optional : analysis of results
#results = list(map(lambda e: e[::-1], graph.collect()))
results = list(dedupJob.map(lambda e: e[::-1]).groupByKey().map(lambda x : (x[0],tuple(x[1]))).collect())
for k in results:
    LOGGER.warn("Component id: "+str(k[0])+"| Number of nodes= "+str(len(k[1])+1))

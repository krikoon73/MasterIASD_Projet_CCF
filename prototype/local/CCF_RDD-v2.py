import findspark
findspark.init() 
from pyspark import SparkConf,SparkContext
import os
import argparse
import subprocess
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
conf.setMaster('local')
conf.setAppName('pyspark-shell-CCF-v2')
# Just for local execution
conf.set('spark.driver.host', '127.0.0.1')
conf.set("spark.ui.proxyBase", "") # Just for having a nice gui locally
os.environ['PYSPARK_PYTHON'] = '/Users/ccompain/.pyenv/versions/miniconda3-latest/bin/python' # Needs to be explicitly provided as env. Otherwise workers run Python 2.7
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

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

# Cleaning - Delete previous results - Only for local execution
subprocess.call(["hdfs", "dfs", "-rm", "-R", output_directory])

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
    reduceJob = mapJob.groupByKey().flatMap(lambda pair: CCF_Iterate_reduce(pair)).sortByKey()
    #print(reduceJob.collect())

    # CCF-dedup 
    dedupJob = reduceJob.distinct()

    # Force the RDD evalusation
    tmp = dedupJob.count()

    # Prepare next iteration
    graph = dedupJob
    new_pair_number = accum.value
    new_pair_flag = bool(new_pair_number)

    LOGGER.warn("Iteration "+str(iteration)+" ===> "+"newPairs = "+str(new_pair_number))

LOGGER.warn("Number of connected components = "+str(tmp))
process_time_checkpoint = time()
process_time = process_time_checkpoint - start_time
LOGGER.warn("Process time = "+str(process_time))
LOGGER.warn("Save last RDD in "+output_directory)
graph.coalesce(1).saveAsTextFile(output_directory)
write_time_checkpoint = time()
write_time = write_time_checkpoint - process_time_checkpoint
LOGGER.warn("RDD write time = "+str(write_time)) 

# Optional : analysis of results
'''
results = list(map(lambda e: e[::-1], graph.collect()))
for k in results:
    LOGGER.debug("Component id: "+str(k[0])+"| Number of nodes= "+str(len(k[1])+1))
'''


import findspark
findspark.init()
#from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as funct
import os
import argparse
import subprocess
from time import time

# Inialize parser and parse argument
parser = argparse.ArgumentParser()
parser.add_argument("-input","--input",help="Complete input file path for Dataset ex. hdfs:/CCF/input/example.csv")
parser.add_argument("-output","--output",help="Complete output path for results ex. hdfs:/CCF/output")
parser.add_argument("-partition","--partition",type=int,help="Number of partitions for dataset")
args = parser.parse_args()
partition_number = args.partition
input_file_path = args.input
output_directory = args.output

# Just for local execution
#os.environ['PYSPARK_PYTHON'] = '/Users/ccompain/.pyenv/versions/miniconda3-latest/bin/python' # Needs to be explicitly provided as env. Otherwise workers run Python 2.7
#os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

sc = SparkSession.builder\
.master("local")\
.appName("pyspark-shell")\
.config("spark.logConf", "true")\
.config("spark.driver.host", "127.0.0.2")\
.getOrCreate() #.config("spark.driver.memory", "16g")
sc.sparkContext.setLogLevel("WARN")

# Initialize logger
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

LOGGER.warn("################################")
LOGGER.warn(" Start CCF DF ")
LOGGER.warn("--------------------------------")

# Import as DF with columns N1 & N2
r =  sc.read\
            .format("csv").option("header", "true")\
            .option("inferSchema", "true")\
            .load(input_file_path).toDF("N1","N2").coalesce(partition_number)

# Cleaning - Delete previous results - Only for local execution
subprocess.call(["hdfs", "dfs", "-rm", "-R", output_directory])

new_pair_flag = True
iteration = 0
#accum = sc.accumulator(0)
graph = r
current_size = graph.count()
number_partition = graph.rdd.getNumPartitions()

LOGGER.warn("Input dataset : "+input_file_path)
LOGGER.warn("Number of pairs = "+str(current_size))
LOGGER.warn("Number of partitions = "+str(number_partition))

start_time = time()

while new_pair_flag:
    iteration += 1
    newPair = False
    newPairs = 0
#    accum.value = 0

    # CCF-iterate (MAP)
    mapJob = graph.union(graph.select('N2', 'N1')).persist()

    # CCF-iterate (REDUCE)
    minDF=mapJob.groupBy('N1').agg(funct.min(mapJob['N2']).alias('minN')).where('min(N2)<N1').persist()
    supplDF=mapJob.join(minDF, "N1").where("minN<>N2").select('N2','minN').withColumnRenamed('N2','N1').withColumnRenamed('minN','N2').persist()
    newPairs=supplDF.count()
    reduceJob = supplDF.union(minDF).persist()

    # CCF-dedup
    dedupJob = reduceJob.distinct().persist()

#    # Force the RDD evalusation
#    tmp = dedupJob.count()

    # Prepare next iteration
    graph = dedupJob
    new_pair_number = newPairs
    new_pair_flag = bool(new_pair_number)

    LOGGER.warn("Iteration "+str(iteration)+" ===> "+"newPairs = "+str(new_pair_number))

process_time_checkpoint = time()
LOGGER.warn("Number of connected components = "+str(dedupJob.count()))
process_time = process_time_checkpoint - start_time
LOGGER.warn("Process time = "+str(process_time))
#LOGGER.warn("Save last DF in "+output_directory)
#graph.coalesce(1).write.csv(output_directory)
#write_time_checkpoint = time()
#write_time = write_time_checkpoint - process_time_checkpoint
#LOGGER.warn("DF write time = "+str(write_time))

# Optional : analysis of results
'''
result= graph.groupBy('N2').count().orderBy('count',ascending=False).withColumnRenamed('N2','Group').withColumnRenamed('count','otherComponentsCount') #.withColumn('nbComponents',count1('count')).select('N2','NbComponents').orderBy('NbComponents',ascending=False)
print(result.show())
'''

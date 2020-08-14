from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as funct
from pyspark.sql.types import IntegerType, StringType
import os
import argparse
from time import time

def min_reduce(N1,N2s):
    min = N1
    for value in N2s:
        if int(value) < int(min):
            min = value
    return min
udf_min_reduce = funct.udf(min_reduce,IntegerType())

def suite_reduce(N1,MinN,N2):
    if int(MinN) == int(N2):
      return N1
    else :
      accum.add(1)
      return N2
udf_suite_reduce = funct.udf(suite_reduce,IntegerType())

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
spark = SparkContext.getOrCreate()
sc = SparkSession.builder\
.appName("pyspark-shell-CCF-DF")\
.getOrCreate() #.config("spark.driver.memory", "16g")

sc.sparkContext.setLogLevel("WARN")
spark.setLogLevel("WARN")

# Initialize logger
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

LOGGER.warn("################################")
LOGGER.warn(" Start CCF DF ")
LOGGER.warn("--------------------------------")

# Import as RDD line_by_line
r = sc.read\
            .format("csv").option("header", "false")\
            .option("delimiter","\t")\
            .option("inferSchema", "true")\
            .load(input_file_path).toDF("N1","N2").coalesce(partition_number)

# CSV transformation -> Separator need to be adapted considering the file format
#r = raw_graph.map(lambda x:x.split('\t')).map(lambda x:(x[0],x[1]))

new_pair_flag = True
iteration = 0
accum = spark.accumulator(0)
graph = r
current_size = graph.count()
number_partition = graph.rdd.getNumPartitions()

LOGGER.warn("Input dataset : "+input_file_path)
LOGGER.warn("Number of pairs = "+str(current_size))
LOGGER.warn("Number of partitions = "+str(number_partition))

start_time = time()

while new_pair_flag:
    iteration += 1
    new_pair_flag = False
    accum.value = 0

    # CCF-iterate (MAP)
    mapJob = graph.union(graph.select('N2', 'N1'))#.persist()

    # CCF-iterate (REDUCE)
    reduceJob=mapJob.groupby("N1").agg(funct.collect_set("N2").alias('N2s'))\
    .withColumn('MinN',udf_min_reduce('N1','N2s')).where('MinN<N1')\
    .withColumn('N2',funct.explode("N2s").alias('N2'))\
    .withColumn('NewN1',udf_suite_reduce('N1','MinN','N2'))\
    .select('NewN1','minN').withColumnRenamed('NewN1','N1').withColumnRenamed('minN','N2').persist()

    # CCF-dedup
    dedupJob = reduceJob.distinct().persist()

#    # Force the RDD evalusation
    tmp = dedupJob.count()

    # Prepare next iteration
    graph = dedupJob
    new_pair_flag = bool(accum.value)
    LOGGER.warn("Iteration "+str(iteration)+" ===> "+"newPairs = "+str(accum.value))

process_time_checkpoint = time()
tmp = dedupJob.count()
LOGGER.warn("Number of connected components = "+str(tmp))
process_time = process_time_checkpoint - start_time
LOGGER.warn("Process time = "+str(process_time))
LOGGER.warn("Save last DF in "+output_directory)
graph.coalesce(1).rdd.saveAsTextFile(output_directory)
write_time_checkpoint = time()
write_time = write_time_checkpoint - process_time_checkpoint
LOGGER.warn("DF write time = "+str(write_time))

# Optional : analysis of results
#results = list(map(lambda e: e[::-1], graph.collect()))
result= graph.groupBy('N2').count().orderBy('count',ascending=False).withColumnRenamed('N2','Group').withColumnRenamed('count','otherComponentsCount') #.withColumn('nbComponents',count1('count')).select('N2','NbComponents').orderBy('NbComponents',ascending=False)
LOGGER.warn(result.show())

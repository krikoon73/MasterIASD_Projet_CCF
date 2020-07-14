key_min=map1.reduceByKey(lambda x,y: min(x,y)).filter(lambda x: x[0]>x[1])
#key_min.collect()

valuelist_min=key_min.join(map1).map(lambda x: (x[0],x[1][1])).cogroup(key_min).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
#valuelist_min.collect()

min_valuelist=valuelist_min.map(lambda x:(x[1][1][0], x[1][0]))
#min_valuelist.take(10)
#use flatmapvalue to transform to RDD (min, value), then filter min != value, map to couple (value, min)
def f(x): return x
min_value=min_valuelist.flatMapValues(f).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[1],x[0]))
#min_value.collect()

unionkeyminvalue=key_min.union(min_value)
#unionkeyminvalue.collect()

#unionkeyminvalue.saveAsTextFile("/Users/ccompain/Documents/code/MasterIASD_sparkProject/github/prototype/output/")
unionkeyminvalue.coalesce(1).saveAsTextFile("hdfs:/CCF/output")
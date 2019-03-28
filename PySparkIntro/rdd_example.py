
from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "count app")

words = sc.parallelize(
    [
        "scala",
        "java",
        "hadoop",
        "spark",
        "akka",
        "spark vs hadoop",
        "pyspark",
        "pyspark and spark"
    ]
)

# count()
# Number of elements
counts = words.count()
print("Number of elements in RDD -> {}".format(counts))


# collect()
# All the elements in the RDD
coll = words.collect()
print("Elements in RDD -> {}".format(coll))


# foreach(f)
# Returns only those elements which meet the condition of the function inside foreach.
# In the following example, we call a print function in foreach,
# which prints all the elements in the RDD.
def f(x): print(x)


fore = words.foreach(f)


# filter()
# A new RDD is returned containing the elements, which satisfies the function inside the filter.
# In the following example, we filter out the strings containing ''spark".
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Filtered RDD -> {}".format(filtered))


# map(f, preservesPartitioning = False)
# A new RDD is returned by applying a function to each element in the RDD.
# In the following example, we form a key value pair and map every string with a value of 1.
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print("Key value pair -> {}".format(mapping))


# reduce(f)
# After performing the specified commutative and associative binary operation, the element in the RDD is returned.
# In the following example, we are importing add package from the operator and applying it on ‘num’
# to carry out a simple addition operation.
nums = sc.parallelize([1, 2, 3, 4, 5])

adding = nums.reduce(add)
print("Adding all the elements -> {}".format(adding))


# join(other, numPartitions = None)
# It returns RDD with a pair of elements with the matching keys and all the values for that particular key.
# In the following example, there are two pair of elements in two different RDDs.
# After joining these two RDDs, we get an RDD with elements having matching keys and their values.
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])

joined = x.join(y)
final = joined.collect()
print("Join RDD -> {}".format(final))


# cache()
# Persist this RDD with the default storage level (MEMORY_ONLY).
# You can also check if the RDD is cached or not.
words = sc.parallelize(
    [
        "scala",
        "java",
        "hadoop",
        "spark",
        "akka",
        "spark vs hadoop",
        "pyspark",
        "pyspark and spark"
    ]
)

words.cache()
caching = words.persist().is_cached
print("Words got cached -> {}".format(caching))


from pyspark import SparkContext

"""
Broadcast variables are used to save the copy of data across all nodes.
This variable is cached on all the machines and not sent on machines with tasks. 

The following example shows how to use a Broadcast variable. 
A Broadcast variable has an attribute called value, 
which stores the data and is used to return a broadcasted value.
"""

sc = SparkContext("local", "Broadcast app")
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])

data = words_new.value
print("Stored data -> {}".format(data))

elem = words_new.value[2]
print("Printing a particular element in RDD -> {}".format(elem))

"""
Accumulator variables are used for aggregating the information through associative and commutative operations.
For example, you can use an accumulator for a sum operation or counters (in MapReduce).

The following example shows how to use an Accumulator variable.
An Accumulator variable has an attribute called value that is similar to what a broadcast variable has. 
It stores the data and is used to return the accumulator's value, but usable only in a driver program.

In this example, an accumulator variable is used by multiple workers and returns an accumulated value.
"""

num = sc.accumulator(10)


def f(x):
    global num
    num += x


rdd = sc.parallelize([20, 30, 40, 50])
rdd.foreach(f)
final = num.value
print("Accumulated value is -> {}".format(final))

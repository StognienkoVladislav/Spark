
from pyspark import SparkContext
from pyspark import SparkFiles

find_distance = 'data/sample'
find_distance_name = 'sample'

sc = SparkContext("local", "SparkFile App")
sc.addFile(find_distance)

print("Absolute Path -> {}".format(SparkFiles.get(find_distance_name)))


from pyspark import SparkContext

sc = SparkContext('local', 'First App')

file_path = 'data/sample'
file_data = sc.textFile(file_path).cache()

print(file_data)

numAs = file_data.filter(lambda s: 'a' in s).count()
numBs = file_data.filter(lambda s: 'b' in s).count()

print("Lines with a: {}, lines with b: {}".format(numAs, numBs))

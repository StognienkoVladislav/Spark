
from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer, PickleSerializer


sc = SparkContext("local", "serialization app", serializer=MarshalSerializer())
print(sc.parallelize(list(range(10000))).map(lambda x: 2*x).take(10))
sc.stop()

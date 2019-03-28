

from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext


sc = SparkContext("local", "Spark Hive App")

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

# spark.sql("LOAD DATA LOCAL INPATH '../data/kv' INTO TABLE src")

# spark.sql("SELECT * FROM src;").show()
spark.sql("show databases").show()
spark.sql("select * from src").show()
# spark.sql("select * from default.employee").show()
# spark.sql("select * from employee limit 1").show()

# employee = hive_context.table("default.employee")
# employee.show()


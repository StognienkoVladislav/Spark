

from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext


sc = SparkContext("local", "Spark Hive App")

spark = SparkSession.builder.\
    master('local').\
    config('spark.sql.warehouse.dir', '/user/hive/warehouse').\
    config('hive.metastore.uris', 'thrift://localhost:9083').\
    enableHiveSupport().\
    getOrCreate()

# spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

# spark.sql("LOAD DATA LOCAL INPATH '../data/kv' INTO TABLE src")

# spark.sql("SELECT * FROM src;").show()
# hive_context = HiveContext(sc)

# check = hive_context.table('test_db.hive_test')
# check.show()

spark.sql("show databases").show()
spark.sql('select * from test_db.hive_test').show()

# spark.sql("select * from src").show()
# spark.sql("select * from default.employee").show()
# spark.sql("select * from employee limit 1").show()

# employee = hive_context.table("default.employee")
# employee.show()


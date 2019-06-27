package spark_vgsales

import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark._

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object spark_bicycle_sharing {

  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark Bicycle Sharing")
      .getOrCreate

    val input = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("bicycle_sharing.csv")

    // Convert these words to lowercase
    input.createTempView("bicycle_sharing")
    val t1 = System.nanoTime
    val sample = spark.sql("Select * from bicycle_sharing where to_station_id = 383")
    sample.coalesce(1).write.format("com.databricks.spark.csv").save("bicycle_sharing_data.csv")
    var duration = (System.nanoTime - t1) / 1e9d
    println(duration)
    System.in.read();
    spark.stop()
  }
}
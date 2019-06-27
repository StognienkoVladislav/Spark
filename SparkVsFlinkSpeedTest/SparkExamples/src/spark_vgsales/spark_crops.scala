package spark_vgsales

import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark._

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object spark_crops {

  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("SparkCSVReader")
      .getOrCreate

    val input = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("fao_data_crops_data.csv")

    // Convert these words to lowercase
    input.createTempView("crops")
    val t1 = System.nanoTime
    val sample = spark.sql("Select * from crops where element_code < 80 and element = 'Area Harvested' and value_footnotes = 'A '")
    sample.coalesce(1).write.format("com.databricks.spark.csv").save("crops_data.csv")
    var duration = (System.nanoTime - t1) / 1e9d
    println(duration)
  }
}
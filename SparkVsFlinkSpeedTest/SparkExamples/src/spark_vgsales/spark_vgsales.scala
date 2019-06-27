package spark_vgsales

import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark._

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object spark_vgsales {

  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("SparkCSVReader")
      .getOrCreate

    val input = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/home/vstohniienko/projects/Spark-Flink/SparkVsFlinkSpeedTest/SparkExamples/vgsales.csv")

    // Convert these words to lowercase
    input.createTempView("vgsales")

    val sample = spark.sql("Select * from vgsales where Publisher='Nintendo' and " +
      "Genre='Action'")
    sample.coalesce(1).write.format("com.databricks.spark.csv").save("/home/vstohniienko/projects/Spark-Flink/SparkVsFlinkSpeedTest/SparkExamples/nintendo_action.csv")

    System.in.read();
    spark.stop()
  }
}
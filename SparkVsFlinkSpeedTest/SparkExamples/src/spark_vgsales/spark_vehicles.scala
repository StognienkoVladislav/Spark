package spark_vgsales

/** Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object spark_vehicles {

  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("SparkCSVReader")
      .getOrCreate

    val input = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/home/vstohniienko/projects/Spark-Flink/SparkVsFlinkSpeedTest/SparkExamples/v2.csv")

    // Convert these words to lowercase
    // For not dropping NA rows
    //input.na.replace(input.columns, Map("" -> "0"))
    input.createTempView("vehicle")
    val sample = spark.sql("Select * from vehicle where speed > 30.0 and rpm > 1300.0 and maf > 20.0 and iat > 40.0")
    sample.coalesce(1).write.format("com.databricks.spark.csv").save("/home/vstohniienko/projects/Spark-Flink/SparkVsFlinkSpeedTest/SparkExamples/vehicle_telematics.csv")

    System.in.read()
    spark.stop()
  }
}
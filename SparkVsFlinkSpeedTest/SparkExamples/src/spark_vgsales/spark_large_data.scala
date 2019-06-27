package spark_vgsales

object spark_large_data {

  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark Large")
      .getOrCreate
    val input = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/home/vstohniienko/projects/gener_datasets/data_16_gb.csv")

    // Convert these words to lowercase
    input.createTempView("sample_large")
    val sample = spark.sql("Select * from sample_large where random_float_1 > 40.0 and random_float_2 < 20.0 and random_int > 400")
    sample.coalesce(1).write.format("com.databricks.spark.csv").save("sample_result_gb.csv")

    System.in.read();
    spark.stop()
  }
}

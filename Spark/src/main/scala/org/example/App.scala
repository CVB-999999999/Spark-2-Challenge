package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]) {

    // TODO: Change to something more dynamic
    val master = "local[*]"
    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master(master).appName("app")
      .getOrCreate()

    // Loads the csv
    var gpsur = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")
    // Test if its reading properly
    //    df1.show()

    /***** Part 1 *****/

    // Replace empty cell with 0
    gpsur = gpsur.na.replace("_c3", Map("nan" -> "0"))
    gpsur = gpsur.na.replace("_c3", Map("null" -> "0"))
    gpsur = gpsur.na.replace("_c3", Map("" -> "0"))

    // Select app name and polarity collumns and cast the last to double
    var df1 = gpsur.select(col("_c0"), col("_c3").cast("double"))

    // Group by app name and show polarity avg
    df1 = df1.groupBy("_c0").avg("_c3")

    // Change column names
    // _c0 -> App 
    // _c3 -> Average_Sentiment_Polarity
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    df1.show()

  }
}

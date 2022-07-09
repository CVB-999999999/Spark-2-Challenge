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

    /* ---- Part 1 ---- */

    // Loads the csv
    /*var gpsur = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

    // Replace empty cell with 0
    gpsur = gpsur.na.replace("_c3", Map("nan" -> "0"))
    gpsur = gpsur.na.replace("_c3", Map("NaN" -> "0"))
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
*/
    //    df1.show()

    /* ---- Part 2 ---- */

    // Loads the csv
    val gps = spark.read.csv("./google-play-store-apps/googleplaystore.csv")

    // Change NaN and other to 0
    var df2 = gps.na.replace("_c2", Map("nan" -> "0"))
    df2 = df2.na.replace("_c2", Map("NaN" -> "0"))
    df2 = df2.na.replace("_c2", Map("null" -> "0"))
    df2 = df2.na.replace("_c2", Map("" -> "0"))

    // Change Rating column to double
    df2 = df2.withColumn("_c2", col("_c2").cast(DoubleType)).as("_c2")

    // Filter Ratings bellow 4
    df2 = df2.filter(df2("_c2") >= 4)

    // Sort by Rating Desc
    df2 = df2.sort(col("_c2").desc)

    // Save to CSV
    // TODO: Change File name
    df2.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .save("best_apps")

  }
}

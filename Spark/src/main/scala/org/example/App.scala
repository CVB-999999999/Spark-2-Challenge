package org.example

import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{aggregate, array_join, col, collect_list, concat_ws, date_format, explode, substring, to_date, upper, when}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType}

/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]) {

    val master = "local[*]"
    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .master(master).appName("app")
      .getOrCreate()

    /* ---- Part 1 ---- */

    // Loads the csv
    var gpsur = spark.read.csv("./google-play-store-apps/googleplaystore_user_reviews.csv")

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

    //    df1.show()

    /* ---- Part 2 ---- */

    // Loads the csv
    var gps = spark.read.csv("./google-play-store-apps/googleplaystore.csv")

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
    df2.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("best_apps")


    /* ---- Part 3 ---- */

    // Create DF with the Categories Arrays
    gps = gps.withColumn("_c3", col("_c3").cast(DoubleType))
    var df3A = gps.groupBy("_c0").agg(concat_ws(",", collect_list(col("_c1"))).as("_c1"))

    // Convert Categories to Array
    df3A = df3A.withColumn("_c1", functions.split(col("_c1"), ",").cast("array<string>"))

    // Create DF with the data the max number of reviews
    var df3B = gps.groupBy("_c0").max("_c3")
    df3B = df3B.toDF("_c0", "_c3")

    // Create a DF with only the lines from the most reviews
    gps.createOrReplaceTempView("gps")
    df3B.createOrReplaceTempView("df3b")

    val df3C = spark.sql("SELECT g.* from gps g, df3b b WHERE g._c3 == b._c3 and g._c0 == b._c0")

    // Create a DF with the categories in a array
    df3C.createOrReplaceTempView("df3c")
    df3A.createOrReplaceTempView("df3a")

    var df3 = spark.sql(
      "SELECT c._c0, a._c1, c._c2, c._c3, c._c4, c._c5, c._c6, c._c7, c._c8, c._c9, c._c10, c._c11, c._c12 " +
        "FROM df3c c, df3a a WHERE a._c0 == c._c0")

    // Change String to the current type
    // Rating
    df3 = df3.withColumn("_c2", col("_c2").cast(DoubleType))
    // Reviews
    df3 = df3.withColumn("_c3", col("_c3").cast(LongType))
    // Size
    df3 = df3.withColumn("_c4",
      when(substring(upper(col("_c4")), -1, 1) === "K", functions.trim(upper(col("_c4")), "K").cast(DoubleType) / 1024)
        .otherwise(functions.trim(upper(col("_c4")), "M")).cast(DoubleType))
    // Price
    df3 = df3.withColumn("_c7", functions.trim(col("_c7"), "$").cast(DoubleType) * 0.9)
    // Genres
    df3 = df3.withColumn("_c9", functions.split(col("_c9"), ";").cast("array<string>"))
    // Date
    df3 = df3.withColumn("_c10", to_date(col("_c10"), "MMMM d, yyyy"))

    // Rename Columns
    df3 = df3.toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    //    df3.sort("_c0").show()

    /* ---- Part 4 ---- */

    // Merge df1 and df3 into df13
    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")

    var df13 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3, df1 WHERE df1.App == df3.App")

    // Save to Parquet
    df13.coalesce(1).write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_cleaned")

    //    df13.show()

    /* ---- Part 5 ---- */

    // Create df4
    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")
    var df4 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3, df1 WHERE df1.App == df3.App")

    // Creates a new DF with the array split into new rows
    df4 = df4.select(col("App"), col("Categories"), col("Rating"), col("Reviews"),
      col("Size"), col("Installs"), col("Type"), col("Price"), col("Content_Rating"),
      explode(col("Genres")).alias("Genres"), col("Last_Updated"),
      col("Current_Version"), col("Minimum_Android_Version"), col("Average_Sentiment_Polarity"))

    df4.show(100)

    // Creates the final DF with right format
    df4 = df4.groupBy("Genres").agg(
      "App" -> "count",
      "Rating" -> "avg",
      "Average_Sentiment_Polarity" -> "avg"
    )

    // Change header for the correct ones
    df4 = df4.toDF("Genre", "Count", "Average_Rating", "Average_Sentiment_Polarity")

    //    df4.agg("Count" -> "sum").show()
    df4.show()

    // Save to Parquet
    df4.coalesce(1).write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_metrics")
  }
}

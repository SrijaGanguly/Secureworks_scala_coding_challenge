import java.io.{File, FileNotFoundException, FileOutputStream}
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

class find_visitors_pipeline_utilities {
  /**
   * The method helps in finding top n visitors and urls for each day in the trace dataframe. Considering null date as valid data.
   * Dataframe returned is sorted by default with most recent date unless otherwise prompted by user.
   * @param df: Its the dataframe of the trace
   * @param top_n: The value n to find that many frequent visitors
   * @param asc_date: The boolean to find if user wants most recent data or the least recent data. This is false for most recent data. By default it is false.
   * @return: A dataframe consisting of the visitors and urls for each day
   */
  def find_top_visitors(df: DataFrame, top_n: Int, asc_date: Boolean = false): DataFrame = {

    // grouping data in df by visitors and date and filtering top n rows for each date.
    // Null dates will be replaced with "NO DATE" for readability and will be grouped into one category.

    println("Processing dataframe to find top visitors for each date")
    var processed_df = df
      .groupBy("dns","date").count()
      .withColumn("row_num", row_number.over(Window.partitionBy("date").orderBy(desc("count"))))
      .filter(col("row_num") <= top_n)
      .withColumnRenamed("count", "visit_count")
      .na.fill("NO DATE", Seq("date"))

    // return least recent visitors else most recent visitors depending on parameter asc_date
    if (asc_date)
    {
      processed_df
        .orderBy(asc("date"), asc("row_num"))
        .select("date","dns", "visit_count")
    }
    else
    {
      processed_df
        .orderBy(desc("date"), asc("row_num"))
        .select("date","dns", "visit_count")
    }

  }

  /**
   * The method writes a the dataframe in csv file under /resources/output directory
   * with a parent directory having a readable timestamp in the name (in MMddyyyy_HHm format)
   * @param df: the dataframe to write as csv
   */
  def write_dataframe(df: DataFrame): Unit = {

    // calculate current system time in MMddyyyy_HHmm format (example - 02182021_2338)
    val timestamp_form = new SimpleDateFormat("MMddyyyy_HHmm")
    val system_timestamp = timestamp_form.format(Calendar.getInstance().getTime())

    // write dataframe under /resources/output/visitor_count_<timestamp> directory for easy access
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("../visitor_count_".concat(system_timestamp))

    //src/main/resources/output

    println("SUCCESS - file saved!")
  }

  /**
   * The following method downloads the file from the url given and returns true if successful.
   * File is downloaded under /resources by the name same name as the gz file
   * @param url: the url to download file
   * @return true if file downloaded successfully else false
   */
  def download_file(url: String): Boolean = {
    try {
      // The file_from_url consists of the stream of bytes from the urls which are copied to the output stream
      // in file_copy_local
      val file_from_url = new URL(url).openStream()
      val file_copy_local = new FileOutputStream("../NASA_access_log_Jul95.gz")
      IOUtils.copy(file_from_url, file_copy_local)
      println("SUCCESS - file downloaded from url!")
      true
    }catch {
      case e:FileNotFoundException => {
        println("ERROR - Something wrong with file download")
        false
      }
      case e: Throwable => {
        println("ERROR - Something else went wrong. Definitely not a FileNotFoundException")
        e.printStackTrace()
        false
      }
    }
  }

  /**
   * Considering the dataframe provided will have the same order of columns to keep things simple,
   * the method transforms the dataframe into usable structure and renames columns
   * @param df: the dataframe to be processed
   * @return processed dataframe with useful structure and column names to further transform
   */
  def apply_custom_schema(df: DataFrame): DataFrame = {
    df
      .drop("_c1", "_c2")
      .withColumnRenamed("_c0", "dns")
      .withColumnRenamed("_c3", "date")
      .withColumnRenamed("_c4", "timediff")
      .withColumnRenamed("_c5", "command")
      .withColumnRenamed("_c6", "res_code")
      .withColumnRenamed("_c7", "some_value")
      .withColumn("timediff", concat(substring(col("date"), 14, 20), split(col("timediff"), "]")(0)))
      .withColumn("date", substring(col("date"), 2, 11))
  }

}

object find_visitors_pipeline {
  val class_object = new find_visitors_pipeline_utilities()

  /**
   * Main method to run the simple pipeline of downloading data, converting to a dataframe with meaningful columns
   * followed by finding top n visitors and urls for each date and writing the dataframe to a csv file
   *
   * @param args : user inputs for "n" ("1" to "5") and "most_recent_first"("Y" or "N") data needed. By default if no argument, its is 1 and "Y" respectively.
   */
  def main(args: Array[String]): Unit = {
    // take command line user input on the number of top visitors to show - int
    val num_of_visitors = if (args.length == 0) "1" else args(0)
    // take command line user input on whether most recent data is needed
    val most_recent_first = if (args.length == 0 || args.length == 1 || args(1).toUpperCase == "Y") "most recent first" else "least recent first"
    println(s"Run mode chosen with top $most_recent_first $num_of_visitors visitors and urls to be displayed")

    // Assumption: running a local spark session
    val spark = SparkSession.builder().appName("CodingChallenge").config("spark.master", "local").getOrCreate()
    // read downloaded file as csv for usable processing
    val lines = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .option("header", "false").load("../NASA_access_log_Jul95.gz")

    // process file into dataframe with meaningful columns.
    // Assumption: Considering the input file remains constant with respect to structure and schema
    val dataframe_from_download = class_object.apply_custom_schema(lines)

    // find the top n visitors and urls for each date in the dataframe processed
    val top_dns_urls = class_object.find_top_visitors(
      dataframe_from_download,
      num_of_visitors.toInt,
      if (most_recent_first == "most recent first") false else true
    )

    // write dataframe into csv under /resources folder
    // Assumption: Considering the location where this code is present has write access
    class_object.write_dataframe(top_dns_urls)
  }
}
package src

import src.Ignore.SparkSessionBuilder
// import src.Databases.MillionSongsDatabase
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) = {
    val sc = SparkSessionBuilder.getContext()
    val sparkSession = SparkSessionBuilder.getSession()

    // Read CSV File
    val df = sparkSession.read
      .option("header", "true")
      .csv(args(0))

    // Write CSV File
    df.write.option("header", "true")
   .csv(args(1) + "dataFrame")
  }
}
  
package src

import src.Ignore.SparkSessionBuilder
import src.Databases.MillionSongsDatabase

object Main {

  def main(args: Array[String]) = {
    val sc = SparkSessionBuilder.getContext()

    // val database = new MillionSongsDatabase(args(1))
    // database.output(sc, args(2) + "database")

    // val bufferedSource = io.Source.fromFile("/tmp/finance.csv")
    // for (line <- bufferedSource.getLines) {
    //     val cols = line.split(",").map(_.trim)
    //     // do whatever you want with the columns here
    //     println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
    // }
    // bufferedSource.close
    // ------- Write Code Here --------
    // val csvFile = sc.textFile(args(1))
    // csvFile.saveAsTextFile(args(2) + "csvFile")

    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x=>x+1).map(_.swap)
    titles.saveAsTextFile(args(2) + "titles")
  }
}
  
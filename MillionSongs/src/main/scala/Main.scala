package src

import src.Ignore.SparkSessionBuilder

object Main {

  def main(args: Array[String]) = {
    val sc = SparkSessionBuilder.getContext()


    // ------- Write Code Here --------
    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x=>x+1).map(_.swap)
    titles.saveAsTextFile(args(2) + "titles")

  }
}
  
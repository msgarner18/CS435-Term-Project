package src

import src.Ignore.SparkSessionBuilder
import src.Validators._
import src.Models._

object Main {

  def main(args: Array[String]) = {
    val sc = SparkSessionBuilder.getContext()
    val sparkSession = SparkSessionBuilder.getSession()

    // Read CSV File
    val df = sparkSession.read
      .option("header", "true")
      .csv(args(0))

    // --- Spilt data into X and Y --- //
    val X = df.drop("Danceability")
    val Y = df.select("SongNumber", "Danceability")

    // --- K-fold Cross Validation --- //
    val Validator = new KFoldCrossValidator(sc, args) // sc and args are passed so I can debug using output files
    val accuracyEstimate = Validator.validate(X, Y, () => new RandomForest)

    sc.parallelize(Seq(accuracyEstimate)).coalesce(1).saveAsTextFile(args(1) + "accuracyEstimate")
  }
}
  
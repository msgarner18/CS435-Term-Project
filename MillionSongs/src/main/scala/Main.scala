package src

import src.Ignore.SparkSessionBuilder
import src.Validators.KFoldCrossValidator
import src.Models.RandomForest
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.spark.sql.DataFrame

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
    val Validator = new KFoldCrossValidator(sc, args, () => new RandomForest)
    val accuracyEstimate = Validator.validate(X, Y)

    // sc.parallelize(Seq(accuracyEstimate)).coalesce(1).saveAsTextFile(args(1) + "accuracyEstimate")
    
    // train and test Random Forest Model 10 times where each fold is used as a test fold and remaining folds are used for training the Random Forest Model
    // average resulting error for each of the 10 tests

    
    // val model = new RandomForest
    // val trainedModel = model.train(X, Y)
    // val modelResult = model.run(X)

    // Write CSV File
    // df.write.option("header", "true").csv(args(1) + "df")

    // X.write.option("header", "true").csv(args(1) + "X")

    // Y.write.option("header", "true").csv(args(1) + "Y")

    // modelResult.write.option("header", "true").csv(args(1) + "modelResult")
  }
}
  
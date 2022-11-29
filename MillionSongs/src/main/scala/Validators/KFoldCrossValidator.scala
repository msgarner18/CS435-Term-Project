package src.Validators

import src.Models._
import org.apache.spark.SparkContext
import scala.util.Random
import org.apache.spark.sql.DataFrame
import scala.math._
import scala.util.control.Breaks._

class KFoldCrossValidator(sc : SparkContext, args: Array[String]) extends Validator {
    def validate(X : DataFrame, Y : DataFrame, modelInitializer : () => Model) : Double = {
        // define constants
        val K = 10
        val N = X.count().asInstanceOf[Int]
        val FOLDSIZE = (N/K).asInstanceOf[Int] + 1

        // Shuffle and partition indexes 1 - 100000
        val indexes = partitionIndexes(N, FOLDSIZE)//List[List[Int]]
        
        // --- Train and run model on K different datasets where i is index of testing data and all other indexes are training data --- //
        val rmseVals = List.range(0, K).map { i =>
            var (xTrain, yTrain, xTest, yTest) = partitionData(X, Y, indexes, i)

            // train and test model
            val model = modelInitializer()
            val trainedModel = model.train(xTrain, yTrain)
            val modelResult = model.run(xTest)

            // calculate error for this test
            val rmse = calculateRMSE(modelResult, yTest)
            // sc.parallelize(Seq(rmse)).coalesce(1).saveAsTextFile(args(1) + "rmse"+i)

            rmse
        }

        // average error for all K tests
        val errorSummary = (rmseVals.sum / K).asInstanceOf[Double]

        errorSummary
    }

    // ---- Private methods ---- //
    private def shuffleIndexes(N : Int) : List[Int] = {
        val unShuffledIndexes = List.range(1, N+1)
        val shuffledIndexes = Random.shuffle(unShuffledIndexes)

        shuffledIndexes
    }

    private def partitionIndexes(N : Int, FOLDSIZE : Int) : List[List[Int]] = {
        val shuffledIndexes = shuffleIndexes(N)
        val indexes = shuffledIndexes.grouped(FOLDSIZE).toList
        
        indexes
    }

    private def partitionData(X : DataFrame, Y : DataFrame, indexes : List[List[Int]], i : Int) : (DataFrame, DataFrame, DataFrame, DataFrame) = {
        // Xtrain and Ytrain are indexes that aren't in list i
        val Xtrain = X.filter(!X("SongNumber").isin(indexes.apply(i): _*))
        val Ytrain = Y.filter(!Y("SongNumber").isin(indexes.apply(i): _*))

        // Xtest and Ytest are indexes in list i
        val Xtest = X.filter(X("SongNumber").isin(indexes.apply(i): _*))
        val Ytest = Y.filter(Y("SongNumber").isin(indexes.apply(i): _*))

        (Xtrain, Ytrain, Xtest, Ytest)
    }

    // Root Mean Squared Error (RMSE)
    private def calculateRMSE(predictedY : DataFrame, Y : DataFrame) : Double = {
        val N = Y.count().asInstanceOf[Int]

        val indexes = Y.select("SongNumber").collect().map(row => row.get(0).toString.toInt)

        var mappedY = dataFrameToMap(Y)
        var mappedPredictedY = dataFrameToMap(predictedY)

        val errors = indexes.map{i =>
            val correct = mappedY.get(i).get
            val predicted = mappedPredictedY.get(i).get

            pow(correct - predicted, 2)
        }

        val rmse = sqrt(errors.sum / N).asInstanceOf[Double]

        rmse
    }

    private def dataFrameToMap(dataFrame : DataFrame) : Map[Int, Double] = {
        var map : Map[Int, Double]= Map()
            dataFrame.collect().foreach(row => {
                val key = row.get(0).toString.toInt
                val value = row.get(1).toString.toDouble
                map += (key -> value)
            })
        
        map
    }
}
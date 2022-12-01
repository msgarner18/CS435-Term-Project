package src

import src.Ignore.SparkSessionBuilder
import src.Validators._
import src.Models._
import org.apache.spark.mllib.linalg._ 
import org.apache.spark.mllib.regression._ 
import org.apache.spark.mllib.evaluation._ 
import org.apache.spark.mllib.tree._ 
import org.apache.spark.mllib.tree.model._ 
import org.apache.spark.rdd._ 
import java.io._
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
object Main {

  def main(args: Array[String]) = {
    val sc = SparkSessionBuilder.getContext()
    val sparkSession = SparkSessionBuilder.getSession()
    // val conf = new Configuration()
    // conf.set("fs.defaultFS", "hdfs://salem.cs.colostate.edu:30101")
    // val fs= FileSystem.get(conf)
    // FileWriter
    // val os = fs.create(new Path("/TermProject/Output/result.txt"))
    // os.write("This is a test".getBytes)
    val outfile = new File("/s/bach/l/under/mselig/CS435-Term-Project/MillionSongs/Output/result.txt")
    val fw = new BufferedWriter(new FileWriter(outfile))
    fw.write("HERE")
  //   // Read CSV File
  //   val df = sparkSession.read
  //     .option("header", "true")
  //     .csv(args(0))

  //   // --- Spilt data into X and Y --- //
  //   val X = df.drop("Danceability")
  //   val Y = df.select("SongNumber", "Danceability")

  //   // --- K-fold Cross Validation --- //
  //   val Validator = new KFoldCrossValidator(sc, args) // sc and args are passed so I can debug using output files
  //   val accuracyEstimate = Validator.validate(X, Y, () => new RandomForest)

  //   sc.parallelize(Seq(accuracyEstimate)).coalesce(1).saveAsTextFile(args(1) + "accuracyEstimate")
  // }


  // RAW DATA
  // 0 SongNumber int 
  // 1 SongID string X
  // 2 AlbumID  int X
  // 3 AlbumName string X
  // 4 ArtistID stringX
  // 5 ArtistLocation stringX
  // 6 ArtistName stringX
  // 7 Danceability double
  // 8 Duration double
  // 9 KeySignature double
  // 10 KeySignatureConfidence double
  // 11 Tempo double
  // 12 TimeSignature double
  // 13 TimeSignatureConfidence double
  // 14 Title stringX
  // 15 Year int
  val rawData = sc.textFile(args(0))
  // data -- one hot encoding -- changed to labled point 
  val data = rawData.map {line => 
      val values = line.split(',').map(_.toDouble) 
      val featureVector = Vectors.dense(values.init) 
      val label = values.last - 1 
      LabeledPoint(label, featureVector) 
  }

  // split data
  val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1)) 
  trainData.cache() 
  cvData.cache() 
  testData.cache()
  // build decisiontree model on training set -- convert with 1-n encoding?

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): 
      MulticlassMetrics = { 
          val predictionsAndLabels = data.map(example => 
          (model.predict(example.features), example.label)) 
          new MulticlassMetrics(predictionsAndLabels) 
      }

  val numClasses = 2022
  val impurity = "gini"
  

  val model = DecisionTree.trainClassifier(trainData, numClasses, Map[Int, Int](), "gini", 4, 100) 
  val metrics = getMetrics(model, cvData)
  println(metrics.precision(_))
  // metrics.confusionMatrix
  // metrics.precision
  // tuning tree?
//   val evaluations = 
//     for (impurity <- Array("gini", "entropy"); 
//         depth <- Array( 1, 20); 
//         bins <- Array( 10, 300)) 
//       yield { 
//           val model = DecisionTree.trainClassifier(trainData, numClasses, Map[Int, Int](), impurity, depth, bins) 
//           val predictionsAndLabels = cvData.map(example =>
//               (model.predict( example.features), example.label) 
//           ) 
//           val acc = 
//               new MulticlassMetrics(predictionsAndLabels).accuracy
//           ((impurity, depth, bins), acc)
//       } 
//   evaluations.sortBy(_._2).reverse.foreach(println)

//   // make forest
// //   // make predictions
//   for (i <- 0 to (evaluations.length -1)){
//         fw.write(evaluations(0).toString())
//   }

 fw.close()
  }


}
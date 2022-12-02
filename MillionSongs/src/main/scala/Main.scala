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
    // val outfile = new File("/s/bach/l/under/mselig/CS435-Term-Project/MillionSongs/Output/result.txt")
    // val fw = new BufferedWriter(new FileWriter(outfile))
    // fw.write("HERE")
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
  //rawData.saveAsTextFile(args(1) + "rawData")
  // data -- one hot encoding -- changed to labled point 
  val data = rawData.map {line => 
      val values = line.split(',').map(_.toDouble) 
      val featureVector = Vectors.dense(values.init) 
      val label = values.last  
      LabeledPoint(label, featureVector) 
  }
  //data.saveAsTextFile(args(1)+"data")
  // split data
  val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1)) 
  trainData.cache() 
  cvData.cache() 
  testData.cache()
  //sc.parallelize(Seq(trainData)).saveAsTextFile(args(1)+"trainData")
  //sc.parallelize(Seq(cvData)).saveAsTextFile(args(1)+"cvData")
  //sc.parallelize(Seq(testData)).saveAsTextFile(args(1)+"testData")
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
  //sc.parallelize(Seq(metrics.precision(_))).saveAsTextFile(args(1)+"result")

  // println("ConfusionMatrix")
  // println(metrics.confusionMatrix)
  // println("Precision")
  // println(metrics.precision(_))
  // tuning tree?
  val evaluations = 
    for (impurity <- Array("gini", "entropy"); 
        depth <- Array( 1, 20); 
        bins <- Array( 10, 300)) 
      yield { 
          val model = DecisionTree.trainClassifier(trainData, numClasses, Map[Int, Int](), impurity, depth, bins) 
          val predictionsAndLabels = cvData.map(example =>
              (model.predict( example.features), example.label) 
          ) 
          val acc = 
              new MulticlassMetrics(predictionsAndLabels).accuracy
          ((impurity, depth, bins), acc)
      } 
      println("Decision Tree")
  evaluations.sortBy(_._2).reverse.foreach(println)
  //sc.parallelize(Seq(evaluations)).saveAsTextFile(args(1)+"eval")
  // make forest
//   // make predictions
  // for (i <- 0 to (evaluations.length -1)){
  //       fw.write(evaluations(0).toString())
  // }

  val forest = RandomForest.trainClassifier( trainData, numClasses, Map[Int, Int](), 20, "auto", "entropy", 30, 300)
  val evaluations2 = 
    for (impurity <- Array("gini", "entropy"); 
        depth <- Array( 1, 5, 10, 20, 30); 
        bins <- Array( 10, 20, 100, 300)) 
      yield { 

          val model = RandomForest.trainClassifier( trainData, numClasses, Map[Int, Int](), 20, "auto", impurity, depth, bins)
          val predictionsAndLabels = cvData.map(example =>
              (model.predict( example.features), example.label) 
          ) 
          val acc = 
              new MulticlassMetrics(predictionsAndLabels).accuracy
          ((impurity, depth, bins), acc)
      } 
      println(" Random Forest")
  evaluations2.sortBy(_._2).reverse.foreach(println)
//  fw.close()

  val input1 = "2,0.0,148.03546,6,0.169,121.274,4,0.384" //1969
  val vector1 = Vectors.dense(input1.split(',').map(_.toDouble)) 
  println("prediction1 -- 1969")
  println(forest.predict(vector1))

  val input2 = "5194,0.0,465.47546,1,0.309,131.999,5,1.0"  //2009
  val vector2 = Vectors.dense(input2.split(',').map(_.toDouble)) 
  println("prediction2 -- 2009")
  println(forest.predict(vector2))

  val input3 = "5121,0.0,89.23383,11,0.727,203.29,4,0.0" //1982
  val vector3 = Vectors.dense(input3.split(',').map(_.toDouble)) 
  println("prediction3 -- 1982")
  println(forest.predict(vector3))

  val input4 = "4949,0.0,149.65506,0,0.0,84.758,1,0.537" //2000
  val vector4 = Vectors.dense(input4.split(',').map(_.toDouble)) 
  println("prediction4 -- 2000")
  println(forest.predict(vector4))

  val input5 = "4676,0.0,342.59546,10,0.0,101.831,4,0.097" //1988
  val vector5 = Vectors.dense(input5.split(',').map(_.toDouble)) 
    println("prediction5 --1988")
  println(forest.predict(vector5))
  }
}
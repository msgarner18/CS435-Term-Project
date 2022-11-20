package src.Databases

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import org.apache.spark.SparkContext

class MillionSongsDatabase(var csvPath : String) {
    private var database = ArrayBuffer[Int]()
    fillDatabase()

    def fillDatabase() = {
        // val m1 = Map("key" -> "value")
        // database += m1
        val num = 1
        database += num
    }

    def output(sc : SparkContext, outputFileName : String) = {
        var rdd = sc.parallelize(database)
        rdd.saveAsTextFile(outputFileName)
    }
}
// package main

import org.apache.spark.sql.SparkSession

object Main {
  // private val 

  def main(args: Array[String]) = {

    // uncomment below line and change the placeholders accordingly
    val sc = SparkSession.builder().master("spark://salem:30361").getOrCreate().sparkContext;
    
    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    // val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
    // val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

  }
}
  
package src.Models

import org.apache.spark.sql.DataFrame

abstract class Model {

    // This method alters internal private variables that are then used in the run method
    def train(X : DataFrame, Y : DataFrame)

    // input X DataFrame
    // output predicted Y DataFrame based on internal private variables that were tuned by the train method
    def run(X : DataFrame) : DataFrame
}
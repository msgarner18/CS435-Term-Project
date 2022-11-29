package src.Models

import org.apache.spark.sql.DataFrame

abstract class Model {
    def train(X : DataFrame, Y : DataFrame)
    def run(X : DataFrame) : DataFrame
}
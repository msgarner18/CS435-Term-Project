package src.Models

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class RandomForest extends Model {
    def train(X : DataFrame, Y : DataFrame) = {

    }

    def run(X : DataFrame) : DataFrame = {
        X.select(col("SongNumber"), lit("1").as("Danceability"))
    }
}
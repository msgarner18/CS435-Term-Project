package src.Validators

import org.apache.spark.sql.DataFrame
import src.Models.Model
import org.apache.spark.SparkContext

// Validators have a validate method
abstract class Validator {

    // input X and Y Dataframes
    // input the function that creates the model you would like to validate
    // outputs a value from 0 to infinite that represents how accurate the model is where 0 is perfectly accurate
    def validate(X : DataFrame, Y : DataFrame, modelInitializer : () => Model) : Double
}
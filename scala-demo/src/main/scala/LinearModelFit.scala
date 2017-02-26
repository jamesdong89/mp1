import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearModelFit {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Temperature Study")

    val sc = new SparkContext(conf)


    val data = sc.textFile("file:///Users/matthewd/projects/mp1/GlobalLandTemperatures/GlobalLandTemperaturesByCity.csv")
    val map = data.flatMap(rec => rec.split(" "))

    val filted = map.filter(rec => rec.split(",").size == 7 && rec.split(",")(1) != null && rec.split(",")(1) != "" && rec.split(",")(1) != "AverageTemperature")

    //linear regression model
    val parsedData = filted.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(1).toDouble, Vectors.dense(parts(0).substring(0, 4).toDouble))
    }.cache()

    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    //    // Save and load model
    //    model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    //    val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    //    print(sameModel.weights)

  }
}

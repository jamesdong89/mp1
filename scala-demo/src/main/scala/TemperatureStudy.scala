import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object TemperatureStudy {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Temperature Study")

    val sc = new SparkContext(conf)
//    val inputPath = args(0)
//    val outputPath = args(1)

    val data = sc.textFile("file:///home/gdong2/GlobalLandTemperaturesByCity.csv")
    println("/********")
    println("*********")
    println("*********")
    println("             Total Tuple Counts: " + data.count())
    println("                                                              *********")
    println("                                                              *********")
    println("                                                              ********/")


    val before = System.currentTimeMillis

    val filted = data.filter(rec => rec.split(",").size == 7 && rec.split(",")(1) != null && rec.split(",")(1) != "" && rec.split(",")(1) != "AverageTemperature")

    //calculate means of temperature by city
    val pair = filted.map(rec => ((rec.split(",")(3), rec.split(",")(4)), List(rec.split(",")(1).toDouble, 1)))

    val reduce = pair.reduceByKey((acc, value) => List(acc(0) + value(0), acc(1) + value(1)))

    val mean = reduce.map(x => (x._2(0)/x._2(1), x._1)).sortByKey()

    val after = System.currentTimeMillis

    println("/********")
    println("*********")
    println("*********")
    println("             Computing Time:" + (after- before))
    println("                                                              *********")
    println("                                                              *********")
    println("                                                              ********/")
    mean.saveAsTextFile("file:///home/gdong2/dataoutput/mean")

  }
}

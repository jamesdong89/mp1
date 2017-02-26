import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object TemperatureStudyMaximum {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Temperature Study")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:///home/gdong2/GlobalLandTemperaturesByCity.csv")
    println("/********")
    println("*********")
    println("*********")
    println("             Total Tuple Counts:" + data.count())
    println("                                                              *********")
    println("                                                              *********")
    println("                                                              ********/")
    val before = System.currentTimeMillis

    val filted = data.filter(rec => rec.split(",").size == 7 && rec.split(",")(1) != null && rec.split(",")(1) != "" && rec.split(",")(1) != "AverageTemperature")

    //calculate means of temperature by city
    val pair = filted.map(rec => ((rec.split(",")(3), rec.split(",")(4)), rec.split(",")(1).toDouble))

    val reduce = pair.max() (new Ordering[Tuple2[(String,String), Double]]() {
      override def compare(x: ((String, String), Double), y:  ((String, String), Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })

    println("/********")
    println("*********")
    println("*********")
    println("             The city with highest temperature: " + reduce)
    println("                                                              *********")
    println("                                                              *********")
    println("                                                              ********/")
    val after = System.currentTimeMillis

    println("/********")
    println("*********")
    println("*********")
    println("             Computing Time:" + (after- before))
    println("                                                              *********")
    println("                                                              *********")
    println("                                                              ********/")
    sc.parallelize(List(reduce)).saveAsTextFile("file:///home/gdong2/dataoutput/maximum")

  }
}

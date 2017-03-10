import org.apache.spark.{SparkConf, SparkContext}

object task3c {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = file2.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (row(15), ((0+row(80)).toDouble, 1)))
    val average_reviews = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2 + b._2))

    average_reviews.foreach(row => println(((row._1),row._2._1/row._2._2)))
  }
}

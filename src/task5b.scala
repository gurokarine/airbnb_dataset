import org.apache.spark.{SparkConf, SparkContext}

object task5b {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val reviews_file = sc.textFile("..\\airbnb_data\\reviews_us.csv")
    val listings_file = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val reviewsData = reviews_file.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val listingsData = listings_file.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val reviews = reviewsData.map(row => (row(0), (row(3), row(4))))
    val cities = listingsData.map(row => (row(43), row(65).replaceAll("[$,]", "").toDouble))

    val city_reviews = cities.join(reviews).map(row => (row._2._2._1+" - "+row._2._2._2, row._2._1))
    val best_reviewers = city_reviews.reduceByKey((a,b) => a + b)
    best_reviewers.map(_.swap).top(1).foreach(println)
  }
}
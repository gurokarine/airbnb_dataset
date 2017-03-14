import org.apache.spark.{SparkConf, SparkContext}

/**
  * For each city, find top 3 hosts with the highest income (throughout
    the whole time of the dataset). Calculate the estimated income based
    on the listing price and number of days it was booked according to
    the calendar dataset.
  */
object task4c {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
    val calendar = sc.textFile("..\\airbnb_data\\calendar_us.csv")

    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val calendarData = calendar.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val calendarMap = calendarData.map(row => (row(0), row(2).replace("f", "1").replace("t", "0").toInt))
    val calendarReduce = calendarMap.reduceByKey((a,b) => a + b)

    val listingsMap = listingsData.map(row => (row(43), (row(28), row(65).replaceAll("[$,]", "").toDouble, row(15))))

    val joining = listingsMap.join(calendarReduce).map(row=> (row._2._1._3, (row._2._1._2 * row._2._2, row._2._1._1)))

    joining.filter(_._1.contains("New York")).map(_.swap).top(3).foreach(println)
    joining.filter(_._1.contains("San Francisco")).map(_.swap).top(3).foreach(println)
    joining.filter(_._1.contains("Seattle")).map(_.swap).top(3).foreach(println)

  }
}

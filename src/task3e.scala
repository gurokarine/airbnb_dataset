import org.apache.spark.{SparkConf, SparkContext}

object task3e {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = file2.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (row(15), ((0+row(80)).toDouble, row(65).replaceAll("[$,]", "").toDouble)))
    val nights_per_year = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2.toDouble + b._2.toDouble))

    val num_nights = nights_per_year.foreach(row => println(row._1,row._2._1*1.3*3*12*row._2._2.toDouble))




  }
}

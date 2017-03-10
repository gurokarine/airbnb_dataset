import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object task3b {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listings_split = file2.filter(line => !line.contains("require_guest_phone_verification")).map( line => line.split("\t") )
    val cities = listings_split.map(row => (row(15) + ", " + row(81),( row(65).replaceAll("[$,]", "").toDouble, 1)))
    val average_price = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2 + b._2))

    val city_roomtype = average_price.foreach(row => println((row._1,row._2._1/row._2._2)))

  }
}
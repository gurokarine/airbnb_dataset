
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Listings from how many and which cities are contained in the dataset?
  */

object task2c {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listings_split = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val cities = listings_split.map(row => (row(15), 1))
    cities.reduceByKey((a,b) => a + b).sortBy(_._2).foreach ( println _)

  }
}

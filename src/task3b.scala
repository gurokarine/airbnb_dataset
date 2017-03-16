import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Average booking price per room type per night
  */

object task3b {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listings_split = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val cities = listings_split.map(row => (row(15) + ": " + row(81),( row(65).replaceAll("[$,]", "").toDouble, 1)))
    val average_price = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2 + b._2))

    val city_roomtype = average_price.map(row => (row._1,row._2._1/row._2._2))

    val toFileStrings = city_roomtype.map(row => row._1+","+row._2).collect()

    val pw = new PrintWriter(new File("csv-files/task3b.csv" ))
    for(line <- toFileStrings){
      pw.write(line+"\n")
    }
    pw.close

  }
}
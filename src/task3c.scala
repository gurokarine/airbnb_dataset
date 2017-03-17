import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Average number of reviews per month
  */

object task3c {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (row(15), ((0+row(80)).toDouble, 1)))
    val average_reviews = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2 + b._2))

    val cities_average_reviews = average_reviews.map(row => (row._1,row._2._1/row._2._2))

    val toFileStrings = cities_average_reviews.map(row => row._1+","+row._2).collect()

    val pw = new PrintWriter(new File("csv-files/task3c.csv" ))
    for(line <- toFileStrings){
      pw.write(line+"\n")
    }
    pw.close
  }
}

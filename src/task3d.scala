import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Estimated number of nights booked per year
  */
object task3d {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (row(15), ((0+row(80)).toDouble, 1)))
    val nights_per_year = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2 + b._2))

    val cities_nights_per_year = nights_per_year.map(row => (row._1,row._2._1*1.3*3*12))

    val toFileStrings = cities_nights_per_year.map(row => row._1+","+row._2).collect()

    val pw = new PrintWriter(new File("csv-files/task3d.csv" ))
    for(line <- toFileStrings){
      pw.write(line+"\n")
    }
    pw.close
  }
}

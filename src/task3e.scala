import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Estimated amount of money spent on AirBnB accommodation per year
  */

object task3e {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (row(15), ((0+row(80)).toDouble, row(65).replaceAll("[$,]", "").toDouble)))
    val nights_per_year = cities.reduceByKey((a,b) => (a._1 + b._1 , a._2.toDouble + b._2.toDouble))

    //Calculating estimated amount of money spent on Airbnb per year:
    // multiplying reviews per month by 1.3, to get a 30% bigger number. Then multiply it by 3, to get three nights booked pr. review, and then by 12 to get a year
    val num_nights = nights_per_year.map(row => (row._1,BigDecimal(row._2._1*1.3*3*12*row._2._2.toDouble)))

    val toFileStrings = num_nights.map(row => row._1+","+row._2).collect()

    val pw = new PrintWriter(new File("csv-files/task3e.csv" ))
    for(line <- toFileStrings){
      pw.write(line+"\n")
    }
    pw.close
  }
}

/**
  * Created by karine on 06.03.2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object scalaTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Load files
    //val file1 = sc.textFile("..\\airbnb_data\\neighborhood_test.csv")
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")
   // val file3 = sc.textFile("..\\airbnb_data\\reviews_us.csv")
    //val file4 = sc.textFile("..\\airbnb_data\\calendar_us.csv")

    //Split each file with tab
   // val neigh_split = file1.map( line => line.split("\t") )
    val listings_split = file2.filter(line => !line.contains("city")).map( line => line.split("\t") )
    //val reviews_split = file3.map( line => line.split("\t") )
    //val calendar_split = file4.map( line => line.split("\t") )


   val cities = listings_split.map(row => (row(15), 1))


   cities.reduceByKey((a,b) => a + b).sortBy(_._2).foreach ( println _)


/*
    val new_york_count = listings_split.filter(line => line.contains("New York")).count()
    val seattle_count = listings_split.filter(line => line.contains("Seattle")).count()
    val san_fransisco_count = listings_split.filter(line => line.contains("San Francisco")).count()

    println("New York: " + new_york_count + " listings")
    println("Seattle: " + seattle_count + " listings")
    println("San Francisco: " + san_fransisco_count + " listings")

*/


  }
}

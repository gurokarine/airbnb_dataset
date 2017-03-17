import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object task5a {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val reviews_file = sc.textFile("..\\airbnb_data\\reviews_us.csv")
    val listings_file = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val reviewsData = reviews_file.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val listingsData = listings_file.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val reviews = reviewsData.map(row => (row(0), (row(3), row(4))))
    val cities = listingsData.map(row => (row(43), row(15)))

    val city_reviews = cities.join(reviews).map(row => (row._2._1 +" - "+ row._2._2._1+" - "+row._2._2._2, 1))
    val best_reviewers = city_reviews.reduceByKey((a,b) => a + b)
    val newYork = best_reviewers.filter(_._1.contains("New York")).map(_.swap).top(3)
    val seattle = best_reviewers.filter(_._1.contains("Seattle")).map(_.swap).top(3)
    val sanFrancisco = best_reviewers.filter(_._1.contains("San Francisco")).map(_.swap).top(3)

    val toFileStringsNY = newYork.map(row => row._2+","+row._1)
    val toFileStringsS = seattle.map(row => row._2+","+row._1)
    val toFileStringsSF = sanFrancisco.map(row => row._2+","+row._1)
    val pw = new PrintWriter(new File("csv-files/task5a.csv" ))
    for(line <- toFileStringsNY){
      pw.write(line+"\n")
    }
    for(line <- toFileStringsS){
      pw.write(line+"\n")
    }
    for(line <- toFileStringsSF){
      pw.write(line+"\n")
    }
    pw.close

  }
}

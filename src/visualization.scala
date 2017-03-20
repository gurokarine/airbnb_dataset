import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

/**
  * Created by karine on 19.03.2017.
  */
object visualization {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listings_split = listings.map(line => line.split("\t"))
    val cities = listings_split.map(row => (row(60).replace(",", " "), (row(51).replace("latitude", "lat"), row(54).replace("longitude", "lon"), row(65), row(64))))


    val toFileStrings = cities.map(a => a._1 + "," + a._2._1 + "," + a._2._2 + "," + a._2._3 + "," + a._2._4).collect()

    val pw = new PrintWriter(new File("csv-files/carto_airbnb_data.csv"))
    for(line <- toFileStrings){
      pw.write(line + "\n")
    }

    pw.close


  }


}

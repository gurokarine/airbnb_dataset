import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

object airbnb {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("AirBNB").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val listings = sc.textFile("D:\\airbnb_datasets\\listings_us.csv")
      val calendar = sc.textFile("D:\\airbnb_datasets\\calendar_us.csv").sample(false , 0.1 , 7)
      val reviews = sc.textFile("D:\\airbnb_datasets\\reviews_us.csv").sample(false , 0.1 , 7)
      val neighborhood = sc.textFile("D:\\airbnb_datasets\\neighborhood_test.csv").sample(false , 0.1 , 7)

      val listingsRdd = listings.map( line => line.split("\t"))
      val calendarRdd = calendar.map( line => line.split("\t"))
      val reviewsRdd = reviews.map( line => line.split("\t"))
      val neighborhoodRdd = neighborhood.map( line => line.split("\t"))

      var nr = 0
      listingsRdd.take(1).map(row =>
        nr = row.length
      )
      val z = new Array[Long](nr)
      val i = 0
      for(i <- 0 to nr-1){
          z(i) = listingsRdd.map(row =>
            if(row.length > i){
              row(i)
            }
          ).distinct.count()
      }

      for (zval <- z){
        println(zval)
      }

      val z2 = new Array[Long](nr)
      z2 = listingsRdd.map(row =>
        for(i <- 0 to row.length-1){
          row(i).distinct.count()
        }
      )



      /*calendarRdd.foreach(row =>
        row.foreach(
          column =>
            println(column)
        )
      )
      reviewsRdd.foreach(row =>
        row.foreach(
          column =>
            println(column)
        )
      )
      neighborhoodRdd.foreach(row =>
        row.foreach(
          column =>
            println(column)
        )
      )*/
    }
}

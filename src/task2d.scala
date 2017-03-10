import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by balde on 10.03.2017.
  */
object task2d {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBNB").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
    val listingsRdd = listings.map(line => line.split("\t"))

    val header = listingsRdd.first()
    val listingsData = listingsRdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    //ID
    val row43 = listingsData.map(row => row(43).toInt)
    val max43 = row43.max()
    val min43 = row43.min()

    //Latitude
    val row51 = listingsData.map(row => row(51).toDouble)
    val max51 = row51.max()
    val min51 = row51.min()

    //Longitude
    val row54 = listingsData.map(row => row(54).toDouble)
    val max54 = row54.max()
    val min54 = row54.min()

    //Price
    val row65 = listingsData.map(row => row(65).replaceAll("[$,]", "").toDouble)
    val max65 = row65.max()
    val min65 = row65.min()

  //Guest included
    val row23 = listingsData.map(row => row(23).toInt)
    val max23 = row23.max()
    val min23 = row23.min()

    println("Id, max: "+max43+", min: "+min43)
    println("Latitude, max: "+max51+", min: "+min51)
    println("Longitude, max: "+max54+", min: "+min54)
    println("Price, max: "+max65+", min: "+min65)
    println("Guests, max: "+max23+", min: "+min23)
  }
}
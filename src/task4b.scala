import org.apache.spark.{SparkConf, SparkContext}

/**
  * Percentage of hosts with more than 1 listings
  */

object task4b {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = file2.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val num_listings = listingsData.map(row => (row(28).toLong, (0+row(31)).toDouble))
    val num_listings_reduce = num_listings.reduceByKey((a,b) => a)
    val num_listings_filter = num_listings_reduce.filter(_._2 > 1.0)

    val total_host = num_listings_reduce.count()
    val host_morethan_1 = num_listings_filter.count()

    println( "Total number of hosts: " + total_host)
    println( "Number of hosts with more than 1 listing: " + host_morethan_1)
    println("Average number of hosts with more than one listing: " + ((host_morethan_1*100)/total_host) + "%")

  }
}

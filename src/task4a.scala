import org.apache.spark.{SparkConf, SparkContext}

/**
  *  Global average number of listings per host
  */

object task4a {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")

    //remove headerline and split lines with tab
    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    //map host_id with 1, and reduce with host id as key
    val num_listings = listingsData.map(row => (row(28).toLong, 1))
    val num_listings_reduce = num_listings.reduceByKey((a,b) => a)

    val num_hosts = num_listings_reduce.count()
    val num_listings_count = num_listings.count()

    println("Number of listings: " + num_listings_count)
    println("Number of hosts: " + num_hosts)
    println("Average: " + (num_listings_count/num_hosts.toDouble))
  }
}

import org.apache.spark.{SparkConf, SparkContext}

object task4a {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = file2.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val num_listings = listingsData.map(row => (row(28).toLong, (0+row(31)).toDouble))
    val num_listings_reduce = num_listings.reduceByKey((a,b) => a)

    val num_hosts = num_listings_reduce.count()
    val num_listings_count = num_listings.count()

    println("Number of listings: " + num_listings_count)
    println("Number of hosts: " + num_hosts)
    println("Average: " + (num_listings_count/num_hosts.toDouble))
  }
}

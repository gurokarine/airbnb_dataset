import org.apache.spark.{SparkConf, SparkContext}

object task4a {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listingsData = file2.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val cities = listingsData.map(row => (0+row(39)).toDouble).count()
    val num_hosts = listingsData.map(row => row(28).toLong).distinct().count()
    println("Number of listings: " + cities)
    println("Number of hosts: " + num_hosts)
    println("Average: " + cities/num_hosts.toDouble)

  }
}

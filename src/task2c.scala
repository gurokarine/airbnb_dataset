
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object task2c {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file2 = sc.textFile("..\\airbnb_data\\listings_us.csv")

    val listings_split = file2.filter(line => !line.contains("city")).map( line => line.split("\t") )
    val cities = listings_split.map(row => (row(15), 1))
    cities.reduceByKey((a,b) => a + b).sortBy(_._2).foreach ( println _)

  }
}

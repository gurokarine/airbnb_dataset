import org.apache.spark.{SparkConf, SparkContext}

object task2b {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("AirBNB").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
      val listingsRdd = listings.map( line => line.split("\t"))

      var nr = 0
      listingsRdd.take(1).map(row =>
        nr = row.length
      )

      val listingsValues = listingsRdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val z = new Array[Long](nr)
      val names = listingsRdd.first()
      val i = 0
      for(i <- 0 to nr-1){
          z(i) = listingsValues.map(row =>
            if(row.length > i){
              row(i)
            }
          ).distinct.count()
      }

      for (i <- 0 to nr-1){
        println(names(i) +": "+z(i))
      }
    }
}

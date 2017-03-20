import org.apache.spark.{SparkConf, SparkContext}
import spray.json._
import scala.collection.mutable.ArrayBuffer
/**
  * Using provided testing set of listings (neighborhood test.csv) with
  * assigned neighborhoods, compare the results with yours and discuss
  * records which do not match. They may be incorrectly assigned either
  * in the provided testing set or in your solution. In how many percent
  * of listings your results agree with the testing set?
  */

object task6a {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val neighborhood_test = sc.textFile("..\\airbnb_data\\neighborhood_test.csv")
    val neighborhood_testData = neighborhood_test.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val listingmap = listingsData.map(row => (row(43).toLong, (row(54).toDouble, row(51).toDouble)))
    val neighbormap = neighborhood_testData.map(row => (row(0).toLong, row(1)))

    val joinmap = listingmap.join(neighbormap).collect()

    //Case-classes for reading the GeoJSON-file.
    case class Properties(
                           neighbourhood: String,
                           neighbourhood_group: Option[String]
                         )
    case class Geometry(
                         coordinates: Seq[Seq[Seq[Seq[Double]]]],
                         `type`: String
                       )

    case class GeoJSON(
                        properties: Properties,
                        geometry: Geometry,
                        `type`: String
                      )

    case class Features(
                         features: Seq[GeoJSON],
                         `type`: String
                       )

    object MyJsonProtocol extends DefaultJsonProtocol {
      implicit val propertiesFormat = jsonFormat2(Properties)
      implicit val geometryFormat = jsonFormat2(Geometry)
      implicit val geojsonFormat = jsonFormat3(GeoJSON)
      implicit val featuresFormat = jsonFormat2(Features)
    }

    import MyJsonProtocol._

    val input = scala.io.Source.fromFile("..\\airbnb_data\\neighbourhoods.geojson")("UTF-8").mkString.parseJson

    val jsonCollection = input.convertTo[Features]

    val features = jsonCollection.features
    var correctCount = 0

    for(testData <- joinmap){
      val point = new Point(testData._2._1._1, testData._2._1._2)
      var foundNeighborhood = ""
      var foundNeighborhood_group = ""
      for (geojson <- features) {
        val name = geojson.properties.neighbourhood
        val group = geojson.properties.neighbourhood_group
        val coords0 = geojson.geometry.coordinates
        val allEdges = new ArrayBuffer[Edge]
        var longmax = -99999.0
        var latmax = -99999.0
        //Goes through all coordinates
        for (coords1 <- coords0) {
          for (coords2 <- coords1) {
            val pstart = new Point(coords2(0)(0), coords2(0)(1))
            var pforrige = new Point(0, 0)
            for (coords3 <- coords2) {
              //Creates an edge for each points after each other in array.
              val p = new Point(coords3(0), coords3(1))
              if (pforrige.lat != 0 && pforrige.long != 0) {
                val edge = new Edge(pforrige, p)
                allEdges += edge
              }
              pforrige = p
              //Store largest longitude and latitude
              longmax = Math.max(longmax, coords3(0))
              latmax = Math.max(latmax, coords3(1))
            }
            //Creates an edge between the first and the last point.
            val edge = new Edge(pforrige, pstart)
            allEdges += edge
          }
        }
        //A point definitly outside of the polygon.
        val outsidePoint = new Point(longmax + 0.01, latmax + 0.01)
        val edge = new Edge(point, outsidePoint)
        val polygon = new Polygon(allEdges.toSeq)
        //Get number of collisions between edge and polygon. If odd number -> Right neighborhood
        val num = polygon.checkCollisions(edge)
        if (num % 2 != 0) {
          foundNeighborhood = name
          foundNeighborhood_group = group.get
        }
      }
      if(testData._2._2.equals(foundNeighborhood)){
        //Count on correct neighborhood
        correctCount = correctCount + 1
      } else if(testData._2._2.equals(foundNeighborhood_group)){
        //Count on correct neighborhoodgroup
        correctCount = correctCount + 1
      }else {
        //Prints out wrong data: TestValue - Found value (Found neighborhoodgroup): Coordinates
        println(testData._2._2+" - "+foundNeighborhood+"("+foundNeighborhood_group+"): "+testData._2._1)
      }
    }
    val count = joinmap.length
    println("Count: "+count)
    println("Correct: "+correctCount)
    println("Correctness: "+(correctCount*100.0/count)+"%")
  }
}

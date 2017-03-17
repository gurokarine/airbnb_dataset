import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.collection.mutable.ArrayBuffer


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

    val sqlContext = new SQLContext(sc)

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
        for (coords1 <- coords0) {
          for (coords2 <- coords1) {
            val pstart = new Point(coords2(0)(0), coords2(0)(1))
            var pforrige = new Point(0, 0)
            for (coords3 <- coords2) {
              val p = new Point(coords3(0), coords3(1))
              if (pforrige.lat != 0 && pforrige.long != 0) {
                val edge = new Edge(pforrige, p)
                allEdges += edge
              }
              pforrige = p

              longmax = Math.max(longmax, coords3(0))
              latmax = Math.max(latmax, coords3(1))
            }
            val edge = new Edge(pforrige, pstart)
            allEdges += edge
          }
        }
        val outsidePoint = new Point(longmax + 0.01, latmax + 0.01)
        val edge = new Edge(point, outsidePoint)
        val polygon = new Polygon(allEdges.toSeq)
        val num = polygon.checkCollisions(edge)
        if (num > 0) {
          if (num % 2 != 0) {
            foundNeighborhood = name
            foundNeighborhood_group = group
          }
        }
      }
      if(testData._2._2.equals(foundNeighborhood)){
        correctCount = correctCount + 1
      } else {
        println(testData._2._2+" - "+foundNeighborhood+": "+testData._2._1)
      }
    }
    val count = joinmap.length
    println("Count: "+count)
    println("Correct: "+correctCount)
    println("Correctness: "+(correctCount*100.0/count)+"%")
  }
}

class Point(var long: Double, var lat: Double){

}
// (Bx - Ax)(Py - By) - (By - Ay)(Px - Bx)
class Edge(var p1: Point, var p2: Point){
  def collides(e: Edge): Boolean = {
    val t11 = (p1.long - p2.long)*(e.p1.lat - p1.lat) - (p1.lat - p2.lat)*(e.p1.long - p1.long)
    val t12 = (p1.long - p2.long)*(e.p2.lat - p1.lat) - (p1.lat - p2.lat)*(e.p2.long - p1.long)
    val t21 = (e.p1.long - e.p2.long)*(p1.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p1.long - e.p1.long)
    val t22 = (e.p1.long - e.p2.long)*(p2.lat - e.p1.lat) - (e.p1.lat - e.p2.lat)*(p2.long - e.p1.long)

    if((t11 >= 0  && t12 < 0) || (t12 >= 0  && t11 < 0)){
      if((t21 >= 0  && t22 < 0) || (t22 >= 0  && t21 < 0)){
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}
class Polygon(var edges: Seq[Edge]){

  def checkCollisions(e: Edge): Int = {
    var collisions = 0
    for(edge <- edges){
      if(e.collides(edge)){
        collisions+=1
      }
    }
    collisions
  }
}

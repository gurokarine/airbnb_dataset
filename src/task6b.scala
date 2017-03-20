import java.io.{File, PrintWriter}
import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spray.json._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object task6b {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val listings = sc.textFile("..\\airbnb_data\\listings_us.csv")
    val listingsData = listings.map(line => line.split("\t")).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val listingmap = listingsData.map(row => ((row(54).toDouble, row(51).toDouble),row(2)))

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

    var neighbourhood_list = new ListBuffer[((Double, Double), String)]()

    for(aListing <- listingmap.collect()){
      val point = new Point(aListing._1._1, aListing._1._2)
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
            if(!group.isEmpty) {
              foundNeighborhood_group = group.get
            }
          }
        }
      }
      val entry = ((aListing._1._1, aListing._1._2), foundNeighborhood)
      neighbourhood_list += entry
    }
    val neighbourhoodRDD = sc.parallelize(neighbourhood_list)
    val joinmap = listingmap.join(neighbourhoodRDD).map(row => (row._2._2, row._2._1.replaceAll("[{}\"]", "").split(",")))
    val merge = joinmap.reduceByKey((a,b) => (a ++ b).distinct)

    val toFileStrings = merge.map(row => row._1+",{"+row._2.mkString(",")+"}").collect()

    val pw = new PrintWriter(new File("csv-files/task6b.csv" ))
    for(line <- toFileStrings){
      pw.write(line+"\n")
    }
    pw.close
  }
}

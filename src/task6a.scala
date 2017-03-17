import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol

import scala.collection.mutable.ArrayBuffer



object task6a {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AirBnB").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    case class Properties(
                           neighbourhood: String,
                           neighbourhood_group: String
                         )
    case class Geometry(
                       coordinates: ArrayBuffer[String],
                       geometry_type: String
                       )

    case class GeoJSON(
                      properties: Properties,
                      geometry: Geometry,
                      geojson_type: String
                      )

    case class Features(
                         features: ArrayBuffer[GeoJSON]
                       )


    /*
    import sqlContext.implicits._
    val df = sqlContext.read.json("..\\airbnb_data\\neighbourhoods.geojson").toDF()
    val newDF = df.select(explode($"features")).toDF("geojson")

    newDF.registerTempTable("geofiles")
    val allNeighbourhoods = sqlContext.sql("SELECT geojson.properties.neighbourhood FROM geofiles")
    allNeighbourhoods.show()
    val list = allNeighbourhoods.collect().toList
    for (row <- list) {
      val theNeighbourhood = sqlContext.sql("SELECT geojson.geometry.coordinates FROM geofiles WHERE " +
        "geojson.properties.neighbourhood LIKE \"" + row(0) + "\"")
      val coords = theNeighbourhood.select(explode($"coordinates")).toDF("coordinates").select(explode($"coordinates")).toDF("coordinates").select(explode($"coordinates")).toDF("coordinates").select(explode($"coordinates")).toDF("coordinates")
      val coordList = coords.collect().toList
      var lastCoord = 0.0
      for (coordRow <- coordList){
        var newCoord = 0.0
        coordRow(0) match {
          case g2: Double => g2
            newCoord = g2
          case _ => throw new ClassCastException
        }
        if(lastCoord == 0.0){
          lastCoord = newCoord
        } else {
          println(lastCoord+" : "+newCoord)
          lastCoord = 0.0


        }
      }
    }
*/
  }
}

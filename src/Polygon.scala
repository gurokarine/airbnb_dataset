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
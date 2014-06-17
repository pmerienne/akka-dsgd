import scala.util.Random


case class Rating(i:Long, j:Long, value: Double)

case class NoMoreRating()

case class RatingBlock(p: Int, q: Int) {
  var ratings:List[Rating] = List()

  def add(rating:Rating) = {
    ratings = rating::ratings
  }

  def shuffle():List[Rating] = {
    Random.shuffle(ratings)
  }
}

import scala.io.Source
import scala.util.Random

case class MovieLensData(trainingPercent:Double = 0.90, filename:String = "src/main/resources/movielens.csv") {
  val (train, test) = Source
    .fromFile(filename)
    .getLines()
    .map(_.split("\t"))
    .map(values => Rating(values(0).toLong, values(1).toLong, values(2).toDouble / 5.0) )
    .toList
    .partition(rating => Random.nextDouble() <  trainingPercent)



}

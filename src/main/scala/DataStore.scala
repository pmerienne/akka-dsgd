import akka.actor.{ActorLogging, Actor}


class DataStore(conf:Conf) extends Actor with ActorLogging {

  val d = conf.d

  var userLatentBlocks:Map[Int, LatentFeatureBlock] = Map()
  var itemLatentBlocks:Map[Int, LatentFeatureBlock] = Map()
  var ratingBlocks:Map[(Int, Int), RatingBlock] = Map()

  private def u(p: Int):LatentFeatureBlock = {
    val fb = LatentFeatureBlock(p, conf)
    userLatentBlocks.getOrElse(p, LatentFeatureBlock(p, conf))
  }

  private def v(q: Int):LatentFeatureBlock = {
    itemLatentBlocks.getOrElse(q, LatentFeatureBlock(q, conf))
  }

  private def b(p:Int, q:Int): RatingBlock = {
    ratingBlocks.getOrElse((p, q), RatingBlock(p, q))
  }

  def store(up:LatentFeatureBlock, vq:LatentFeatureBlock) {
    log.debug("Storing blocks {}, {}", up.blockIndex, vq.blockIndex)
    userLatentBlocks += (up.blockIndex -> up)
    itemLatentBlocks += (vq.blockIndex -> vq)
  }

  def predict(i: Long, j: Long): Double = {
    log.debug("Predicting {}, {}", i, j)
    val (p, q) = assignBlock(i, j);
    u(p).features(i).t * v(q).features(j)
  }

  def store(rating:Rating) {
    log.debug("Storing rating {}", rating)
    val (p, q) = assignBlock(rating.i, rating.j);

    val block = b(p, q)
    block.add(rating)

    ratingBlocks += ((p, q) -> block)
  }

  def assignBlock(i:Long, j:Long): (Int, Int) = ((i % d).toInt,(j % d).toInt)

  def rmse(ratings: List[Rating]): Double = {
    var n = 0.0
    var sum = 0.0

    ratings.foreach(rating => {
      n += 1.0
      val prediction = predict(rating.i, rating.j)
      sum += Math.pow((rating.value - prediction), 2.0)
      log.debug("Prediction : {}, Actual : {}", prediction, rating.value)
    })

    Math.sqrt(sum / n)
  }

  def receive = {
    case (rating:Rating) => store(rating)
    case BlockToProcess(p, q, iteration) => sender ! BlockData(p, q, iteration, u(p), v(q), b(p, q))
    case data:BlockData => store(data.up, data.vq)
    case RMSE(ratings) => sender ! rmse(ratings)
  }
}

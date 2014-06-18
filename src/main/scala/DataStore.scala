import breeze.linalg.DenseVector

case class BlockData(p:Int, q:Int, up:LatentFeatureBlock, vq:LatentFeatureBlock, ratingBlock:RatingBlock)

case class DataStore(conf:Conf) {

  val d = conf.d

  var userLatentBlocks:Map[Int, LatentFeatureBlock] = Map()
  var itemLatentBlocks:Map[Int, LatentFeatureBlock] = Map()
  var ratingBlocks:Map[(Int, Int), RatingBlock] = Map()


  def data(p:Int, q:Int): BlockData = {
    val up = userLatentBlocks.getOrElse(p, LatentFeatureBlock(p, conf))
    val vq = itemLatentBlocks.getOrElse(q, LatentFeatureBlock(q, conf))
    val ratingBlock = ratingBlocks.getOrElse((p, q), RatingBlock(p, q))
    BlockData(p, q, up, vq, ratingBlock)
  }

  def store(up:LatentFeatureBlock, vq:LatentFeatureBlock) {
    userLatentBlocks += (up.blockIndex -> up)
    itemLatentBlocks += (vq.blockIndex -> vq)
  }

  def store(rating:Rating) {
    val (p, q) = assignBlock(rating.i, rating.j);

    val block = ratingBlocks.getOrElse((p, q), RatingBlock(p, q))
    block.add(rating)

    ratingBlocks += ((p, q) -> block)
  }

  def features(i:Long, j:Long):(DenseVector[Double], DenseVector[Double]) = {
    val (p, q) = assignBlock(i, j);

    val up = userLatentBlocks.getOrElse(p, LatentFeatureBlock(p, conf))
    val vq = itemLatentBlocks.getOrElse(q, LatentFeatureBlock(q, conf))
    (up.features(i), vq.features(j))
  }

  def assignBlock(i:Long, j:Long): (Int, Int) = ((i % d).toInt,(j % d).toInt)


}

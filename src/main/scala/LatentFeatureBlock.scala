import breeze.linalg.DenseVector

case class LatentFeatureBlock(blockIndex:Int, conf:Conf) {

  var latentVectors:Map[Long, DenseVector[Double]] = Map()

  val k = conf.k

  def features(index:Long):DenseVector[Double] = {
    latentVectors.getOrElse(index, randomFeatures)
  }

  def features(index:Long, features:DenseVector[Double]) = {
    latentVectors += (index -> features)
  }

  def randomFeatures: DenseVector[Double] = {
    DenseVector.rand[Double](k) .* (1.0/k)
  }
}

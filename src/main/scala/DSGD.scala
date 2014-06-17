import akka.actor.{ActorLogging, Props, Actor, ActorRef}
import akka.routing.RoundRobinRouter
import scala.util.Random


case class StartDSGD()

case class BlockToProcess(p:Int, q:Int, iteration:Int)
case class ProcessedBlock(p:Int, q:Int)

case class BlockData(p:Int, q:Int, iteration:Int, up:LatentFeatureBlock, vq:LatentFeatureBlock, ratingBlock:RatingBlock)

class DSGDMaster(matrixStore: ActorRef, conf:Conf) extends Actor with ActorLogging {
  val workerRouter = context.actorOf(Props(new SGDWorker(matrixStore, self, conf)).withRouter(RoundRobinRouter(conf.nbWorkers)), name = "workerRouter")

  var unlockedQs:Set[Int] = Set()
  var unlockedPs:Set[Int] = Set()

  var iterationCounts:Map[(Int, Int), Int] = Map()

  def receive = {
    case StartDSGD() => start()
    case ProcessedBlock(p, q) => {
      log.debug("Received processed {}, {}", p, q)
      unlock(p, q)

      val iteration = iterationCounts.getOrElse((p, q), 0) + 1
      iterationCounts += ((p, q) -> iteration)

      emitUnlockedBlocks()
    }
  }

  def start() = {
    log.info("Starting dsgd with {}", conf)
    unlockedPs = List.range(0, conf.d).toSet
    unlockedQs = List.range(0, conf.d).toSet
    emitUnlockedBlocks()
  }

  def unlock(p:Int, q:Int) = {
    log.debug("Unlocking {}, {}", p, q)
    unlockedPs += p
    unlockedQs += q
  }

  private def emitUnlockedBlocks() = {
    while(!unlockedPs.isEmpty && !unlockedQs.isEmpty) {
      val block = randomBlock()
      log.debug("Sending {} to process", block)
      workerRouter ! block
    }
  }


  private def randomBlock():BlockToProcess = {
    val p:Int = Random.shuffle(unlockedPs.toList).head
    val q:Int = Random.shuffle(unlockedQs.toList).head
    val iteration = iterationCounts.getOrElse((p, q), 0)

    lock(p, q)
    BlockToProcess(p, q, iteration)
  }

  private def lock(p:Int, q:Int) {
    unlockedPs -= p
    unlockedQs -= q
  }
}


class SGDWorker(dataStore: ActorRef, master: ActorRef, conf:Conf) extends Actor with ActorLogging {

  val λ = conf.λ
  val η = conf.η

  def update(data:BlockData) = {
    val step =  2 * η / (data.iteration + 1)

    val ratings = data.ratingBlock.shuffle()
    ratings.foreach(rating => {
      val ui = data.up.features(rating.i)
      val vj = data.vq.features(rating.j)

      val eui = rating.value - ui.t * vj

      val newUi = ui + (vj * eui - ui * λ) * step
      val newVj = vj + (ui * eui - vj * λ) * step

      data.up.features(rating.i, newUi)
      data.vq.features(rating.j, newVj)
    })
  }


  def receive = {
    case BlockToProcess(p, q, iteration) => dataStore ! BlockToProcess(p, q, iteration)
    case data:BlockData=> {
      log.debug("Processing {}, {}", data.p, data.q)
      update(data)
      log.debug("Process of {}, {} done", data.p, data.q)

      dataStore ! data
      master ! ProcessedBlock(data.p, data.q)
    }
  }
}
import akka.actor._
import akka.routing.RoundRobinRouter
import scala.util.Random

import scala.concurrent.duration._

case class StartDsgd()
case class DsgdFinished()

case class BlockToProcess(p:Int, q:Int, iteration:Int)
case class ProcessedBlock(p:Int, q:Int, iteration:Int)

case class EmitBlock()

class DsgdMaster(dataStore: DataStore, conf:Conf) extends Actor with ActorLogging {

  val workerRouter = context.actorOf(Props(new SgdWorker(dataStore, conf)).withRouter(RoundRobinRouter(conf.nbWorkers)), name = "workerRouter")

  var unlockedQs:Set[Int] = Set()
  var unlockedPs:Set[Int] = Set()

  var starter:ActorRef = null

  var availableBlocks:Set[BlockToProcess] = Set()
  var processingBlocks:Set[BlockToProcess] = Set()

  def receive = {
    case EmitBlock() => emitRandomBlock()
    case StartDsgd() => start()
    case ProcessedBlock(p, q ,iteration) => unlock(ProcessedBlock(p, q ,iteration))
  }

  def start() = {
    log.info("Starting dsgd with {}", conf)
    unlockedPs = List.range(0, conf.d).toSet
    unlockedQs = List.range(0, conf.d).toSet
    availableBlocks = (for(p <- 0 until conf.d;q <- 0 until conf.d) yield BlockToProcess(p, q, 0)).toSet

    starter = sender
    (0 until conf.nbWorkers).foreach(i => self ! EmitBlock())
  }

  def unlock(block:ProcessedBlock) {
    log.debug("Received processed {}", block)

    unlockedPs += block.p
    unlockedQs += block.q

    processingBlocks -= BlockToProcess(block.p, block.q, block.iteration)

    if (block.iteration < conf.iterations) {
      availableBlocks += BlockToProcess(block.p, block.q, block.iteration + 1)
    }

    self ! EmitBlock()
  }

  private def emitRandomBlock() = {
    log.debug("Emiting block")
    val block = Random.shuffle(availableBlocks.toList).find(block => unlockedPs.contains(block.p) && unlockedQs.contains(block.q))
    block.map(block => {
      availableBlocks -= block
      processingBlocks += block
      lock(block.p, block.q)
      log.debug("Sending {} to process", block)
      workerRouter ! block
    })

    if(starter != null && availableBlocks.isEmpty && processingBlocks.isEmpty) {
      starter ! DsgdFinished()
    }
  }

  private def lock(p:Int, q:Int) {
    unlockedPs -= p
    unlockedQs -= q
  }
}


class SgdWorker(dataStore: DataStore, conf:Conf) extends Actor with ActorLogging {

  val λ = conf.λ
  val η = conf.η

  def update(data:BlockData, iteration:Int) = {
    val step =  2 * η / (iteration + 1)

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
    case BlockToProcess(p, q, iteration) => {
      val start = System.currentTimeMillis()
      log.debug("Processing {}, {}, {}", p, q, iteration)

      val data = dataStore.data(p, q)
      update(data, iteration)

      dataStore.store(data.up, data.vq)

      log.debug(s"Process of ${p}, ${q}, ${iteration} done in ${System.currentTimeMillis() - start}ms")

      sender ! ProcessedBlock(data.p, data.q, iteration)
    }
  }
}
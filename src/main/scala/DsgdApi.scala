import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

case class RMSERequest(ratings:List[Rating])

case class Conf(k:Int = 20, η:Double = 0.2, λ:Double = 0.01, d:Int = 8, iterations:Int = 10, nbWorkers:Int = 8)

case class DsgdApi(conf:Conf = Conf()) {

  implicit val timeout = Timeout(30 minutes)

  val system = ActorSystem("DSGD")
  val dataStore = DataStore(conf)
  val master = system.actorOf(Props(new DsgdMaster(dataStore, conf)), name = "dsgd-master")

  def start():Future[Any] = {
    master ? StartDsgd()
  }

  def add(rating:Rating) = {
    dataStore.store(rating)
  }

  def shutdown() = {
    system.shutdown();
  }

  def rmse(ratings: List[Rating]): Double = {
    var n = 0.0
    var sum = 0.0

    ratings.foreach(rating => {
      n += 1.0
      val prediction = predict(rating.i, rating.j)
      sum += Math.pow((rating.value - prediction), 2.0)
    })

    Math.sqrt(sum / n)
  }

  def predict(i: Long, j: Long): Double = {
    val (ui, vj) = dataStore.features(i, j)
    ui.t * vj
  }


}

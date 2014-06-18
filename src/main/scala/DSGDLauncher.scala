import akka.actor._
import scala.concurrent.duration._

case class Conf(k:Int = 10, η:Double = 0.1, λ:Double = 0.1, iterations:Int = 30, d:Int = 5, nbWorkers:Int = 8)

class DSGDLauncher(master:ActorRef, dataStore:ActorRef) extends Actor with ActorLogging {

  def receive() = {
    case NoMoreRating() => master ! StartDSGD()
    case Rating(i, j, value) => dataStore ! Rating(i, j, value)
  }
}

object DSGDLauncher {


  def launch(ratings:List[Rating], conf:Conf = Conf()):ActorRef = {
    val system = ActorSystem("DSGD")

    val dataStore = system.actorOf(Props(new DataStore(conf)), name = "data-store")
    val master = system.actorOf(Props(new DSGDMaster(dataStore, conf)), name = "dsgd-master")
    val api = system.actorOf(Props(new DsgdApi(dataStore)), name = "dsgd-api")

    val launcher = system.actorOf(Props(new DSGDLauncher(master, dataStore)), name = "dsgd-launcher")

    ratings.foreach(rating => launcher ! rating)
    launcher ! NoMoreRating()


    return api
  }


}

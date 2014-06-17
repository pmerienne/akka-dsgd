import akka.actor.{Actor, ActorRef, ActorLogging}

case class RMSE(ratings:List[Rating])

class DsgdApi(dataStore: ActorRef) extends Actor with ActorLogging {

  def receive = {
    case Rating(i, j, value) => dataStore ! Rating(i, j , value)
    case RMSE(ratings) => dataStore ! RMSE(ratings)
    case rmse:Double => log.info("RMSE : " + rmse)
  }

}

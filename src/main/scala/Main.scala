import akka.actor.{ActorSystem, Props, ActorRef}

object Main extends App {

  val conf = Conf(15, 0.01, 0.01, 30, 10, 10)
  val data = MovieLensData();

  val api = DSGDLauncher.launch(data.train, conf)


  while(true) {
    api ! RMSE(data.test)
    Thread.sleep(1000)
  }


}



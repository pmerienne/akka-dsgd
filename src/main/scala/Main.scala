import akka.actor.{ActorSystem, Props, ActorRef}

object Main extends App {

  val conf = Conf(15, 0.1, 0.1, 5, 3, 8)
  val data = MovieLensData();

  val api = DSGDLauncher.launch(data.train, conf)


  while(true) {
    Thread.sleep(3000)
    api ! RMSE(data.test)
  }


}



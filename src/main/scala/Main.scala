import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  val conf: Conf = Conf(k=50, η= 0.2, λ= 0.01, d=8, iterations= 30, nbWorkers= 8)
  val dsgd = DsgdApi(conf)

  val data = MovieLensData()
  data.train.foreach(rating => dsgd.add(rating))

  val startTime = System.currentTimeMillis()
  dsgd.start() onComplete {
    case Failure(failure) => {
      println("Unable to finish DSGD : " + failure)
      dsgd.shutdown()
    }

    case Success(result)  => {
      println(s"DSGD finished in ${System.currentTimeMillis() - startTime}ms")

      val rmse = dsgd.rmse(data.test)
      println(s"RMSE : ${rmse * 100}%")
      dsgd.shutdown()
    }
  }

}



package week3

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.routing.RoundRobinPool
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import scala.collection.mutable.{ListBuffer, Stack}
import scala.util.Random.nextDouble

class excludeBadWords {

}

object badWordsDictionary {
  def bad_words(): List[String] = {
    val badWordsListB: ListBuffer[String] = new ListBuffer[String]
    badWordsListB += "shit"
    badWordsListB += "ass"
    badWordsListB += "fuck"
    badWordsListB += "bastard"
    badWordsListB += "cunt"
    badWordsListB += "dick"
    badWordsListB += "fuck off"
    badWordsListB += "fuck you"
    badWordsListB += "bitch"
    badWordsListB += "whore"
    badWordsListB += "slut"
    val badWordsList = badWordsListB.toList
    return badWordsList
  }
}

class pool_supervisor extends Actor with ActorLogging {
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2) {
    case _ => log.info("A printer actor has been killed"); Restart
  }
  def receive = {
    case list: List[String] => {
      if (list(1).toInt < 37) {
        val workerActors = context.actorOf(Props[printerActor].withRouter(RoundRobinPool(7)), name = "WorkerActors")
        context.watch(workerActors)
        val message: String = list.head
        if (message == " panic") {
          workerActors ! PoisonPill
        } else {
          workerActors ! message
        }
      }
      if (list(1).toInt >= 37 && list(1).toInt < 44) {
        val workerActors = context.actorOf(Props[printerActor].withRouter(RoundRobinPool(5)), name = "WorkerActors")
        context.watch(workerActors)
        val message: String = list(0)
        if (message == " panic") {
          workerActors ! PoisonPill
        } else {
          workerActors ! message
        }
      }
      if (list(1).toInt >= 44 && list(1).toInt < 57) {
        val workerActors = context.actorOf(Props[printerActor].withRouter(RoundRobinPool(3)), name = "WorkerActors")
        context.watch(workerActors)
        val message: String = list(0)
        if (message == " panic") {
          workerActors ! PoisonPill
        } else {
          workerActors ! message
        }
      }
      if (list(1).toInt >= 57) {
        val workerActors = context.actorOf(Props[printerActor].withRouter(RoundRobinPool(2)), name = "WorkerActors")
        context.watch(workerActors)
        val message: String = list(0)
        if (message == " panic") {
          workerActors ! PoisonPill
        } else {
          workerActors ! message
        }
      }
    }
    case Terminated(workerActors) => {
      val newPrinter = ActorSystem().actorOf(Props(new printerActor))
      println("\u001B[32m"+"The printer actor has been restarted."+ "\u001B[0m")
    }
  }
}

class taskManager extends Actor {
  def receive = {
    case m: String => {
      val poolActor = ActorSystem().actorOf(Props(new pool_supervisor))
      var sleepTime: Int = poissonDistribution.distribution(50)
      Thread.sleep(sleepTime)
      var sleepT = Integer.toString(sleepTime)
      val listB: ListBuffer[String] = new ListBuffer[String]
      listB += m
      listB += sleepT
      val list = listB.toList
      poolActor ! list
    }
  }
}

class mediatorActor extends Actor {
  def receive = {
    case list: List[String] => {
      val tMActor = ActorSystem().actorOf(Props(new taskManager))
      val st :Stack[String] = new Stack[String]()
      for (i<-0 to list.length-1) {
        st.push(list(i))
      }
      val st_inverse :Stack[String] = new Stack[String]()
      for (j<-0 to st.length-1) {
        st_inverse.push(st.pop())
      }
      for (k<-0 to st_inverse.length-1) {
        var tweet: String = st_inverse.pop()
        tMActor ! tweet
      }
    }
  }
}

class printerActor extends Actor {
  def receive = {
    case m: String => {
      println(m)
    }
  }
}

object poissonDistribution {
  def distribution(i: Double): Int = {
    val limit: Double = Math.exp(-i)
    var p: Double = nextDouble()
    var n: Int = 0
    while(p >= limit) {
      n = n+1
      p = p * nextDouble()
    }
    return n
  }
}

class reader_1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get();
      val mediatorActor = ActorSystem().actorOf(Props(new mediatorActor))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split(":")
      for (i<-0 to split1.length-1) {
        if (split1(i).contains("text") && split1(i+1).contains("source") && i < split1.length-1 || split1(i).contains("panic} event")){
          val s2 = split1(i+1).split("\"")
          val s3 = s2(1).split(" ")
          tweetsListB += s2(1)
          for (j<-0 to s3.length-1) {
            for (k<-0 to badWordsDictionary.bad_words().length-1) {
              if (s3(j)==badWordsDictionary.bad_words()(k)) {
                val s4 = s3(j).split("")
                for (l<-0 to s4.length-1) {
                  s4(l) = "\u001B[31m"+"*"+ "\u001B[0m"
                }
                s3(j) = s4.mkString("")
              }
            }
          }
          s2(1) = s3.mkString(" ")
        }
        if(split1(i).contains("panic} event")) {
          val s2 = split1(i).split("}")
          tweetsListB += s2(0)
        }
      }
      val tweetsList = tweetsListB.toList
      mediatorActor ! tweetsList
    }
  }
}

class reader_2 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/2").get();
      val mediatorActor = ActorSystem().actorOf(Props(new mediatorActor))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split(":")
      for (i<-0 to split1.length-1) {
        if (split1(i).contains("text") && split1(i+1).contains("source") && i < split1.length-1){
          val s2 = split1(i+1).split("\"")
          val s3 = s2(1).split(" ")
          for (j<-0 until s3.length) {
            for (k<-badWordsDictionary.bad_words().indices) {
              if (s3(j)==badWordsDictionary.bad_words()(k)) {
                val s4 = s3(j).split("")
                for (l<-0 to s4.length-1) {
                  s4(l) = "\u001B[31m"+"*"+ "\u001B[0m"
                }
                s3(j) = s4.mkString("")
              }
            }
          }
          s2(1) = s3.mkString(" ")
          tweetsListB += s2(1)
        }
      }
      val tweetsList = tweetsListB.toList
      mediatorActor ! tweetsList
    }
  }
}

object worker_tweets extends App {
  val system = ActorSystem()
  val reader1Actor = system.actorOf(Props(new reader_1))
  val reader2Actor = system.actorOf(Props(new reader_2))
  reader1Actor ! "Start"
  reader2Actor ! "Start"
}


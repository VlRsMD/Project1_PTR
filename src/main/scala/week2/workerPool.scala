package week2

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import scala.collection.mutable.{ListBuffer, Stack}
import scala.util.Random.nextDouble

class workerPool {

}

class poolSupervisor extends Actor with ActorLogging {
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2) {
    case _ => log.info("A printer actor has been killed"); Restart
  }
  val printer1 = context.actorOf(Props[Printer], "printer1")
  val printer2 = context.actorOf(Props[Printer], "printer2")
  val printer3 = context.actorOf(Props[Printer], "printer3")
  context.watch(printer1)
  context.watch(printer2)
  context.watch(printer3)
  def receive = {
    case m: String => {
      printer1 ! m
      if (m== " panic") {
        printer1 ! PoisonPill
      }
    }
    case Terminated(printer1) => {
      val newPrinter1 = ActorSystem().actorOf(Props(new Printer))
      println("The printer actor has been restarted.")
    }
    case Terminated(printer2) => {
      val newPrinter2 = ActorSystem().actorOf(Props(new Printer))
      println("The printer actor has been restarted.")
    }
    case Terminated(printer3) => {
      val newPrinter2 = ActorSystem().actorOf(Props(new Printer))
      println("The printer actor has been restarted.")
    }
  }
}

class mediator extends Actor {
  def receive = {
    case list: List[String] => {
      val poolActor = ActorSystem().actorOf(Props(new poolSupervisor))
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
        poolActor ! tweet
      }
      poolActor ! 0
    }
  }
}

class Printer extends Actor {
  def receive = {
    case m: String => {
      var sleepTime: Int = poisson_distribution.distribution(50)
      Thread.sleep(sleepTime)
      println(m)
    }
  }
}

object poisson_distribution {
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

class readTweets_1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get();
      val mediatorActor = ActorSystem().actorOf(Props(new mediator))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split(":")
      for (i<-0 to split1.length-1) {
        if (split1(i).contains("text") && split1(i+1).contains("source") && i < split1.length-1 || split1(i).contains("panic} event")){
          val s2 = split1(i+1).split("\"")
          tweetsListB += s2(1)
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

class readTweets_2 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/2").get();
      val mediatorActor = ActorSystem().actorOf(Props(new mediator))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split(":")
      for (i<-0 to split1.length-1) {
        // println(spl1(i))
        if (split1(i).contains("text") && split1(i+1).contains("source") && i < split1.length-1){
          val s2 = split1(i+1).split("\"")
          tweetsListB += s2(1)
        }
      }
      val tweetsList = tweetsListB.toList
      mediatorActor ! tweetsList
    }
  }
}

object worker extends App {
  val system = ActorSystem()
  val reader1Actor = system.actorOf(Props(new readTweets_1))
  val reader2Actor = system.actorOf(Props(new readTweets_2))
  reader1Actor ! "Start"
  reader2Actor ! "Start"
}

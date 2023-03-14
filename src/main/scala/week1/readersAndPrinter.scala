package week1

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import akka.actor._
import scala.collection.mutable.ListBuffer
import scala.util.Random.nextDouble

class readersAndPrinter {

}

class read_tweets1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get();
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
      for (k<-0 to tweetsList.length-1) {
        val printerActor = ActorSystem().actorOf(Props(new print_tweet))
        printerActor ! tweetsList(k)
      }
    }
  }
}

class read_tweets2 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/2").get();
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
      for (k<-0 to tweetsList.length-1) {
        val printerActor = ActorSystem().actorOf(Props(new print_tweet))
        printerActor ! tweetsList(k)
      }
    }
  }
}

class print_tweet extends Actor {
  def receive = {
    case m: String => {
      var sleepTime: Int = distribution.poisson_distribution(50)
      Thread.sleep(sleepTime)
      println(m)
    }
  }
}

object distribution {
  def poisson_distribution(i: Double): Int = {
    val limit: Double = Math.exp(-i)
    var prod: Double = nextDouble()
    var n: Int = 0
    while(prod >= limit) {
      n = n+1
      prod = prod * nextDouble()
    }
    return n
  }
}

object read_and_print extends App {
  val system = ActorSystem()
  val reader1Actor = system.actorOf(Props(new read_tweets1))
  val reader2Actor = system.actorOf(Props(new read_tweets2))
  reader1Actor ! "Start"
  reader2Actor ! "Start"
}

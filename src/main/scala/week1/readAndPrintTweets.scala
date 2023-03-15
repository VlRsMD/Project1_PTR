package week1

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import akka.actor._
import scala.collection.mutable.ListBuffer

class readAndPrintTweets {

}

class readTweets1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get();
      val printerActor = ActorSystem().actorOf(Props(new printTweet))
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
        printerActor ! tweetsList(k)
      }
    }
  }
}

class printTweet extends Actor {
  def receive = {
    case m: String => {
      println(m)
    }
  }
}

object readAndPrint extends App {
  val system = ActorSystem()
  val readerActor = system.actorOf(Props(new readTweets1))
  readerActor ! "Start"
}

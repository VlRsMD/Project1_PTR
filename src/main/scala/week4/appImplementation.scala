package week4

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.routing.RoundRobinPool
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import requests.Response
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random.nextDouble

class appImplementation {

}

class reader1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get()
      val mediatorActor = ActorSystem().actorOf(Props(new mediator_actor))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split("event: \"message\"")
      for (i<-0 until split1.length) {
        tweetsListB += split1(i)
      }
      val tweetsList = tweetsListB.toList
      mediatorActor ! tweetsList
    }
  }
}

class reader2 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/2").get()
      val mediatorActor = ActorSystem().actorOf(Props(new mediator_actor))
      val tweetsListB: ListBuffer[String] = new ListBuffer[String]
      val split1 = tweets.text().split("event: \"message\"")
      for (i<-0 until split1.length) {
        tweetsListB += split1(i)
      }
      val tweetsList = tweetsListB.toList
      mediatorActor ! tweetsList
    }
  }
}

class mediator_actor extends Actor {
  def receive = {
    case list: List[String] => {
      val engagementRatioPool = context.actorOf(Props[engagementRatioActor].withRouter(RoundRobinPool(3)))
      val st :mutable.Stack[String] = new mutable.Stack[String]()
      for (i<-list.indices) {
        st.push(list(i))
      }
      val st_inverse :mutable.Stack[String] = new mutable.Stack[String]()
      for (j<-st.indices) {
        st_inverse.push(st.pop())
      }
      for (k<-st_inverse.indices) {
        var tweet: String = st_inverse.pop()
        engagementRatioPool ! tweet
      }
    }
  }
}

class engagementRatioActor extends Actor {
  def receive = {
    case m: String => {
      val tweetInfoListB: ListBuffer[String] = new ListBuffer[String]
      val sentimentScorePool = context.actorOf(Props[sentimentScoreActor].withRouter(RoundRobinPool(3)))
      if (m.contains("\"favourites_count\"") && m.contains("\"followers_count\"") && m.contains("\"retweet_count\"") && m.contains("\"name\":\"")) {
        val split = m.split(":")
        var tweet: String = ""
        var favouritesCount: Double = 0
        var followersCount: Double = 0
        var retweetCount: Double = 0
        var engagementRatio: Double = 0
        var tweet1: String = ""
        for(j<-0 until split.length) {
          if(j!=split.length-1 && split(j).contains("\"favourites_count\"")) {
            val s3 = split(j+1).split(",\"")
            favouritesCount = s3(0).toDouble
          }
        }
        for(j<-0 until split.length) {
          if(j!=split.length-1 && split(j).contains("\"followers_count\"")) {
            val s3 = split(j+1).split(",\"")
            followersCount = s3(0).toDouble
          }
        }
        for(j<-0 until split.length) {
          if(j!=split.length-1 && split(j).contains("\"retweet_count\"")) {
            val s3 = split(j+1).split(",\"")
            retweetCount = s3(0).toDouble
          }
        }
        engagementRatio = (favouritesCount + retweetCount) / followersCount
        val splitText = m.split("\"text\":\"")
        val splitTweet = splitText(1).split("\",\"source\":")
        val s3 = splitTweet(0).split(" ")
        for (m<-0 until s3.length) {
          for (k<-bad_words_dictionary.bad_words().indices) {
            if (s3(m)==bad_words_dictionary.bad_words()(k)) {
              val s4 = s3(m).split("")
              for (l<-0 to s4.length-1) {
                s4(l) = "\u001B[31m"+"*"+ "\u001B[0m"
              }
              s3(m) = s4.mkString("")
            }
          }
        }
        splitTweet(0) = s3.mkString(" ")
        tweet = splitTweet(0)
        val split1 = m.split("\"name\":\"")
        val split2 = split1(1).split("\",\"created_at\"")
        val name = split2(0)
        tweetInfoListB += name
        tweetInfoListB += tweet
        tweetInfoListB += engagementRatio.toString
        val tweetInfoList = tweetInfoListB.toList
        sentimentScorePool ! tweetInfoList
      } else if(m.contains("{\"message\": panic}")) {
        val split1 = m.split(":")
        val split2 = split1(2).split(" ")
        val split3 = split2(1).split("}")
        val panicMessage = split3(0)
        tweetInfoListB += panicMessage
        val tweetInfoList = tweetInfoListB.toList
        sentimentScorePool ! tweetInfoList
      }
    }
  }
}

class sentimentScoreActor extends Actor {
  def receive = {
    case list: List[String] => {
      val tweetsInfoListB: ListBuffer[String] = new ListBuffer[String]
      var panicMessage: String = ""
      if (list.head.equals("panic")) {
        panicMessage = list.head
        tweetsInfoListB += panicMessage
        val tweetsInfoList = tweetsInfoListB.toList
        val taskManagerActor = ActorSystem().actorOf(Props(new task_manager))
        taskManagerActor ! tweetsInfoList
      } else if (list.length == 3) {
        var sentimentScore: Double = 0;
        var emotionsSum: Int = 0;
        val emotions = emotionValues.getEmotionValues()
        val split = list.head.split(" ")
        for (i <- split.indices) {
          for (j <- emotions.indices) {
            if(split(i) == emotions(j).head) {
              val emotionValue = emotions(j)(1).toInt
              emotionsSum = emotionsSum + emotionValue
              sentimentScore = emotionsSum.toDouble / split.length.toDouble
            }
          }
        }
        tweetsInfoListB += list.head
        tweetsInfoListB += list(1)
        tweetsInfoListB += list(2)
        tweetsInfoListB += sentimentScore.toString
        val tweetsInfoList = tweetsInfoListB.toList
        val taskManagerPool = context.actorOf(Props[task_manager].withRouter(RoundRobinPool(3)))
        taskManagerPool ! tweetsInfoList
      }
    }
  }
}

class task_manager extends Actor {
  def receive = {
    case list: List[String] => {
      val workerPools = context.actorOf(Props[worker_pool].withRouter(RoundRobinPool(3)), name = "worker_pools")
      var sleepTime: Int = PoissonDistribution.distribution(50)
      var sleepT = Integer.toString(sleepTime)
      val newListB: ListBuffer[String] = new ListBuffer[String]
      if(list.head.equals("panic")) {
        newListB += list.head
        val newList = newListB.toList
        Thread.sleep(sleepTime)
        workerPools ! newList
      } else {
        newListB += list.head
        newListB += list(1)
        newListB += list(2)
        newListB += list(3)
        newListB += sleepT
        val newList = newListB.toList
        Thread.sleep(sleepTime)
        workerPools ! newList
      }
    }
  }
}

class worker_pool extends Actor with ActorLogging {
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2) {
    case _ => log.info("A printer actor has been killed"); Restart
  }
  def receive = {
    case list: List[String] => {
      if(list.length == 1) {
        val workerActors = context.actorOf(Props[printer_actor].withRouter(RoundRobinPool(1)), name = "WorkerActors")
        context.watch(workerActors)
        val message: String = list.head
        if (message == "panic") {
          workerActors ! PoisonPill
        }
      } else if (list(4).toInt < 37) {
        val workerActors = context.actorOf(Props[printer_actor].withRouter(RoundRobinPool(7)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += "User name: " + list.head
        newListB += "Tweet: " + "\"" + list(1) + "\""
        newListB += "Engagement Ratio: " + list(2)
        newListB += "Sentiment Score: " + list(3)
        val newList = newListB.toList
        workerActors ! newList
      } else if (list(4).toInt >= 37 && list(4).toInt < 44) {
        val workerActors = context.actorOf(Props[printer_actor].withRouter(RoundRobinPool(5)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += "User name: " + list.head
        newListB += "Tweet: " + "\"" + list(1) + "\""
        newListB += "Engagement Ratio: " + list(2)
        newListB += "Sentiment Score: " + list(3)
        val newList = newListB.toList
        workerActors ! newList
      } else if (list(4).toInt >= 44 && list(4).toInt < 57) {
        val workerActors = context.actorOf(Props[printer_actor].withRouter(RoundRobinPool(3)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += "User name: " + list.head
        newListB += "Tweet: " + "\"" + list(1) + "\""
        newListB += "Engagement Ratio: " + list(2)
        newListB += "Sentiment Score: " + list(3)
        val newList = newListB.toList
        workerActors ! newList
      } else if (list(4).toInt >= 57) {
        val workerActors = context.actorOf(Props[printer_actor].withRouter(RoundRobinPool(2)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += "User name: " + list.head
        newListB += "Tweet: " + "\"" + list(1) + "\""
        newListB += "Engagement Ratio: " + list(2)
        newListB += "Sentiment Score: " + list(3)
        val newList = newListB.toList
        workerActors ! newList
      }
    }
    case Terminated(workerActors) => {
      val newPrinter = ActorSystem().actorOf(Props(new printer_actor))
      println("\u001B[32m"+"The printer actor has been restarted."+ "\u001B[0m")
      println
    }
  }
}


class printer_actor extends Actor {
  def receive = {
    case list: List[String] => {
      for(i<-list.indices) {
        println(list(i))
      }
      println
    }
  }
}

object PoissonDistribution {
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

object emotionValues {
  def getEmotionValues(): List[List[String]] = {
    val emotionsListB: ListBuffer[List[String]] = new ListBuffer[List[String]]
    var resp: Response = requests.get("http://localhost:4000/emotion_values")
    val split = resp.data.toString().split("\n")
    var splitLength: Int = split.length
    for(i<-0 until splitLength) {
      val listB: ListBuffer[String] = new ListBuffer[String]
      if(i != splitLength-1) {
        val split2 = split(i).split("")
        var j: Int = split2.length-2
        var k: Int = j-1
        var s: Int = k-2
        if(split2(k)=="-") {
          var score: String = split2(k)+split2(j)
          var text: String = ""
          for(h<-0 to k-2) {
            text = text + split2(h)
          }
          listB += text
          listB += score
          val list = listB.toList
          emotionsListB += list
        } else {
          var score: String = split2(j)
          var text: String = ""
          for(h<-0 until k) {
            text = text + split2(h)
          }
          listB += text
          listB += score
          val list = listB.toList
          emotionsListB += list
        }
      } else {
        val split2 = split(i).split("")
        var j: Int = split2.length-1
        var k: Int = j-1
        var s: Int = k-2
        if(split2(k)=="-") {
          var score: String = split2(k)+split2(j)
          var text: String = ""
          for(h<-0 to k-2) {
            text = text + split2(h)
          }
          listB += text
          listB += score
          val list = listB.toList
          emotionsListB += list
        } else {
          var score: String = split2(j)
          var text: String = ""
          for(h<-0 until k) {
            text = text + split2(h)
          }
          listB += text
          listB += score
          val list = listB.toList
          emotionsListB += list
        }
      }
    }
    val emotionsList = emotionsListB.toList
    return emotionsList
  }
}

object bad_words_dictionary {
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
    badWordsListB += "bitches"
    badWordsListB += "whore"
    badWordsListB += "slut"
    val badWordsList = badWordsListB.toList
    return badWordsList
  }
}

object app extends App {
  val system = ActorSystem()
  val reader1Actor = system.actorOf(Props(new reader1))
  val reader2Actor = system.actorOf(Props(new reader2))
  reader1Actor ! "Start"
  reader2Actor ! "Start"
}


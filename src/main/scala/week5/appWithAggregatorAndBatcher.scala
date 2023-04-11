package week5

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.routing.RoundRobinPool
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import requests.Response
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random.nextDouble

class appWithAggregatorAndBatcher {

}

class tweetsReaderNr1 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/1").get()
      val mediatorActor = ActorSystem().actorOf(Props(new tweetsMediator))
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

class tweetsReaderNr2 extends Actor {
  def receive = {
    case "Start" => {
      var tweets: Document = Jsoup.connect("http://localhost:4000//tweets/2").get()
      val mediatorActor = ActorSystem().actorOf(Props(new tweetsMediator))
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

class tweetsMediator extends Actor {
  def receive = {
    case list: List[String] => {
      val panic = ActorSystem().actorOf(Props(new panicActor))
      val tweetTextPool = context.actorOf(Props[tweetTextActor].withRouter(RoundRobinPool(3)))
      val engagementRatioPool = context.actorOf(Props[sentimentScoreCalculator].withRouter(RoundRobinPool(3)))
      val sentimentScorePool = context.actorOf(Props[engagementRatioCalculator].withRouter(RoundRobinPool(3)))
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
        panic ! tweet
        tweetTextPool ! tweet
        engagementRatioPool ! tweet
        sentimentScorePool ! tweet
      }
    }
  }
}

class panicActor extends Actor {
  def receive = {
    case m: String => {
      val workerActor = ActorSystem().actorOf(Props(new workerPoolActor))
      if(m.contains("{\"message\": panic}")) {
        val split1 = m.split(":")
        val split2 = split1(2).split(" ")
        val split3 = split2(1).split("}")
        val panicMessage = split3(0)
        workerActor ! panicMessage
      }
    }
  }
}

class tweetTextActor extends Actor {
  def receive = {
    case m: String => {
      val tweetTextInfoListB: ListBuffer[String] = new ListBuffer[String]
      val taskManager = context.actorOf(Props[taskManagerActor].withRouter(RoundRobinPool(5)))
      if (m.contains("\"favourites_count\"") && m.contains("\"followers_count\"") && m.contains("\"retweet_count\"")) {
        val splitData = m.split("\"text\":\"")
        val splitTweet = splitData(1).split("\",\"source\":")
        val s3 = splitTweet(0).split(" ")
        for (m<-0 until s3.length) {
          for (k<-dictionaryOfBadWords.bad_words().indices) {
            if (s3(m)==dictionaryOfBadWords.bad_words()(k)) {
              val s4 = s3(m).split("")
              for (l<-0 to s4.length-1) {
                s4(l) = "\u001B[31m"+"*"+ "\u001B[0m"
              }
              s3(m) = s4.mkString("")
            }
          }
        }
        splitTweet(0) = s3.mkString(" ")
        val tweet = splitTweet(0)
        val splitTweetForTimestamp0 = m.split("\"unix_timestamp_100us\": ")
        val splitTweetForTimestamp01 = splitTweetForTimestamp0(1).split("}")
        val timestamp = splitTweetForTimestamp01(0)
        tweetTextInfoListB += "Tweet Text"
        tweetTextInfoListB += tweet
        tweetTextInfoListB += timestamp
        val tweetTextInfoList = tweetTextInfoListB.toList
        taskManager ! tweetTextInfoList
      }
    }
  }
}

class engagementRatioCalculator extends Actor {
  def receive = {
    case m: String => {
      val engagementRatioInfoListB: ListBuffer[String] = new ListBuffer[String]
      val taskManager = context.actorOf(Props[taskManagerActor].withRouter(RoundRobinPool(5)))
      if (m.contains("\"favourites_count\"") && m.contains("\"followers_count\"") && m.contains("\"retweet_count\"")) {
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
        val splitTweetForTimestamp0 = m.split("\"unix_timestamp_100us\": ")
        val splitTweetForTimestamp01 = splitTweetForTimestamp0(1).split("}")
        val timestamp = splitTweetForTimestamp01(0)
        engagementRatioInfoListB += "Engagement Ratio"
        engagementRatioInfoListB += engagementRatio.toString
        engagementRatioInfoListB += timestamp
        val engagementRatioInfoList = engagementRatioInfoListB.toList
        taskManager ! engagementRatioInfoList
      }
    }
  }
}


class sentimentScoreCalculator extends Actor {
  def receive = {
    case m: String => {
      val sentimentScoreInfoListB: ListBuffer[String] = new ListBuffer[String]
      val taskManager = context.actorOf(Props[taskManagerActor].withRouter(RoundRobinPool(5)))
      if (m.contains("\"favourites_count\"") && m.contains("\"followers_count\"") && m.contains("\"retweet_count\"")) {
        val splitData = m.split("\"text\":\"")
        val splitTweet = splitData(1).split("\",\"source\":")
        val s3 = splitTweet(0).split(" ")
        for (m<-0 until s3.length) {
          for (k<-dictionaryOfBadWords.bad_words().indices) {
            if (s3(m)==dictionaryOfBadWords.bad_words()(k)) {
              val s4 = s3(m).split("")
              for (l<-0 to s4.length-1) {
                s4(l) = "\u001B[31m"+"*"+ "\u001B[0m"
              }
              s3(m) = s4.mkString("")
            }
          }
        }
        splitTweet(0) = s3.mkString(" ")
        val tweet = splitTweet(0)
        var sentimentScore: Double = 0;
        var emotionsSum: Int = 0;
        val emotionValues = emotions.getEmotionValues()
        val split = tweet.split(" ")
        for (i <- split.indices) {
          for (j <- emotionValues.indices) {
            if(split(i) == emotionValues(j).head) {
              val emotionValue = emotionValues(j)(1).toInt
              emotionsSum = emotionsSum + emotionValue
              sentimentScore = emotionsSum.toDouble / split.length.toDouble
            }
          }
        }
        val splitTweetForTimestamp0 = m.split("\"unix_timestamp_100us\": ")
        val splitTweetForTimestamp01 = splitTweetForTimestamp0(1).split("}")
        val timestamp = splitTweetForTimestamp01(0)
        sentimentScoreInfoListB += "Sentiment Score"
        sentimentScoreInfoListB += sentimentScore.toString
        sentimentScoreInfoListB += timestamp
        val sentimentScoreInfoList = sentimentScoreInfoListB.toList
        taskManager ! sentimentScoreInfoList
      }
    }
  }
}

class taskManagerActor extends Actor {
  def receive = {
    case list: List[String] => {
      val workerPools = context.actorOf(Props[workerPoolActor].withRouter(RoundRobinPool(3)), name = "worker_pools")
      var sleepTime: Int = poissonDistributionClass.distribution(50)
      var sleepT = Integer.toString(sleepTime)
      val newListB: ListBuffer[String] = new ListBuffer[String]
      newListB += list.head
      newListB += list(1)
      newListB += list(2)
      newListB += sleepT
      val newList = newListB.toList
      Thread.sleep(sleepTime)
      workerPools ! newList
    }
  }
}

class workerPoolActor extends Actor with ActorLogging {
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 2) {
    case _ => log.info("A printer actor has been killed"); Restart
  }
  val aggregatorActor = ActorSystem().actorOf(Props(new aggregatorActor1))
  context.watch(aggregatorActor)
  def receive = {
    case "panic" => {
        aggregatorActor ! PoisonPill
    }
    case list: List[String] => {
      if (list(3).toInt < 37) {
        val workerActors = context.actorOf(Props[aggregatorActor1].withRouter(RoundRobinPool(7)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += list.head
        newListB += list(1)
        newListB += list(2)
        val newList = newListB.toList
        workerActors ! newList
      }
      if (list(3).toInt >= 37 && list(3).toInt < 44) {
        val workerActors = context.actorOf(Props[aggregatorActor1].withRouter(RoundRobinPool(5)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += list.head
        newListB += list(1)
        newListB += list(2)
        val newList = newListB.toList
        workerActors ! newList
      }
      if (list(3).toInt >= 44 && list(3).toInt < 57) {
        val workerActors = context.actorOf(Props[aggregatorActor1].withRouter(RoundRobinPool(3)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += list.head
        newListB += list(1)
        newListB += list(2)
        val newList = newListB.toList
        workerActors ! newList
      }
      if (list(3).toInt >= 57) {
        val workerActors = context.actorOf(Props[aggregatorActor1].withRouter(RoundRobinPool(2)), name = "WorkerActors")
        context.watch(workerActors)
        val newListB: ListBuffer[String] = new ListBuffer[String]
        newListB += list.head
        newListB += list(1)
        newListB += list(2)
        val newList = newListB.toList
        workerActors ! newList
      }
    }
    case Terminated(workerActors) => {
      val newAggregator = ActorSystem().actorOf(Props(new aggregatorActor1))
      println("\u001B[32m"+"The aggregator actor has been restarted."+ "\u001B[0m")
      println
    }
  }
}

class aggregatorActor1 extends Actor {
  val aggregator2 = context.actorOf(Props[aggregatorActor2].withRouter(RoundRobinPool(2)))
  def receive = {
    case list: List[String] => {
      if(list.head.equals("Tweet Text")) {
        tweetsApp.tweetTextsListB += list
      }
      if(list.head.equals("Engagement Ratio")) {
        tweetsApp.engagementRatioListB += list
      }
      if(list.head.equals("Sentiment Score")) {
        tweetsApp.sentimentScoreListB += list
      }
    }
  }
  val tweetTextsList = tweetsApp.tweetTextsListB.toList
  val engagementRatioList = tweetsApp.engagementRatioListB.toList
  val sentimentScoreList = tweetsApp.sentimentScoreListB.toList
  val allListsB :ListBuffer[List[List[String]]] = new ListBuffer[List[List[String]]]
  allListsB += tweetTextsList
  allListsB += engagementRatioList
  allListsB += sentimentScoreList
  val allLists = allListsB.toList
  aggregator2 ! allLists
}

class aggregatorActor2 extends Actor {
  def receive = {
    case list: List[List[List[String]]] => {
      val batcherActor = context.actorOf(Props[Batcher].withRouter(RoundRobinPool(3)))
      val tweetTextList = list.head
      val engagementRatioList = list(1)
      val sentimentScoreList = list(2)
      val tweetInfoListB :ListBuffer[List[String]] = new ListBuffer[List[String]]
      for (i<-0 until tweetTextList.length) {
        for (j<-0 until engagementRatioList.length) {
          for (k<-0 until sentimentScoreList.length) {
            if(sentimentScoreList(k)(2).equals(engagementRatioList(j)(2)) && sentimentScoreList(k)(2).equals(tweetTextList(i)(2)) && engagementRatioList(j)(2).equals(tweetTextList(i)(2))) {
              val infoListB: ListBuffer[String] = new ListBuffer[String]
              infoListB += "\u001B[33m" + "Tweet Information: " + "\u001B[0m"
              infoListB += "Tweet text: " + "\"" + tweetTextList(i)(1) + "\""
              infoListB += "Engagement ratio: " + engagementRatioList(j)(1)
              infoListB += "Sentiment score: " + sentimentScoreList(k)(1)
              tweetInfoListB += infoListB.toList
            }
          }
        }
      }
      val tweetInfoList = tweetInfoListB.toList
      for (i<-0 until tweetInfoList.length) {
        batcherActor ! tweetInfoList(i)
      }
    }
  }
}

class Batcher extends Actor {
  def receive = {
    case list: List[String] => {
      for (i<-list.indices) {
        println(list(i))
      }
      println
      if(list.isEmpty) {
        val sleepTime : Int = poissonDistributionClass.distribution(30)
        Thread.sleep(sleepTime)
        println("\u001B[33m"+"Tweet Information: "+"\u001B[0m"+"\n"+"Tweet text: --- "+"\n"+"Engagement Ratio: --- "+"\n"+"Sentiment Score: --- ")
      }
    }
    case None => {
      val sleepTime : Int = poissonDistributionClass.distribution(30)
      Thread.sleep(sleepTime)
      println("\u001B[33m"+"Tweet Information: "+"\u001B[0m"+"\n"+"Tweet text: --- "+"\n"+"Engagement Ratio: --- "+"\n"+"Sentiment Score: --- ")
    }
  }
}

object poissonDistributionClass {
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

object emotions {
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

object dictionaryOfBadWords {
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

object tweetsApp extends App {
  val system = ActorSystem()
  val reader1Actor = system.actorOf(Props(new tweetsReaderNr1))
  val reader2Actor = system.actorOf(Props(new tweetsReaderNr2))
  val tweetTextsListB :ListBuffer[List[String]] = new ListBuffer[List[String]]
  val engagementRatioListB :ListBuffer[List[String]] = new ListBuffer[List[String]]
  val sentimentScoreListB :ListBuffer[List[String]] = new ListBuffer[List[String]]
  reader1Actor ! "Start"
  reader2Actor ! "Start"
}



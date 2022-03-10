package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._


import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val master = opt[String](default=Some(""))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args) 

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator())

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

  val globalAvg = train.map(elem => elem.rating).sum() / train.count()

  var allUserAvg = Map(0 -> 0.0)

  var allItemAvg = Map(0 -> 0.0)

  var allItemDev = Map(0 -> 0.0)

  def distribUserAvg(df : RDD[Rating], user : Int): Double = {
    if (allUserAvg.get(user) != None)
      allUserAvg(user)
    else
      {
      val dfFiltered = train.filter(elem => elem.user == user)
      var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg

      else
        tmp = dfFiltered.map(elem => elem.rating).sum() / dfFiltered.count()

      allUserAvg = allUserAvg.updated(user, tmp)
      tmp
    }
  }

  def distribItemAvg(df : RDD[Rating], item : Int): Double = {
    if (allItemAvg.get(item) != None)
      allItemAvg(item)
    else 
      {
      val dfFiltered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg
      else
        tmp = dfFiltered.map(elem => elem.rating).sum() / dfFiltered.count()

      allItemAvg = allItemAvg.updated(item, tmp)
      tmp
      }
  }

  
  def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  def distribAvgDev(df : RDD[Rating], item : Int): Double = {
    if (allItemDev.get(item) != None)
        allItemDev(item)
    else {
      var tmp = df.filter(l => l.item == item).map{(elem) => 
        val userAvg = distribUserAvg(df, elem.user)
        (elem.rating - userAvg) / scale(elem.rating, userAvg)}.sum / df.filter(l => l.item == item).count()
      allItemDev =  allItemDev.updated(item, tmp)
      tmp
    }
  }

  def distribpredicted(user: Int, item : Int, df : RDD[Rating]): Double = {
    val userAvg = distribUserAvg(df , user)
    if (df.filter(l => l.item == item).isEmpty)
      userAvg
    else {
      val avgDev = distribAvgDev(df, item)
      userAvg + avgDev * scale((userAvg + avgDev), userAvg)
    }
  }

  def mae(df_test : RDD[Rating], df_train : RDD[Rating]): Double = {
    df_test.map{ elem =>
      abs(elem.rating - distribpredicted(elem.user, elem.item, df_train)
    }
  }
  // Save answers as JSON

  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(globalAvg), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(distribUserAvg(train, 1)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(distribItemAvg(train, 1)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(distribAvgDev(train, 1)), // Datatype of answer: Double,
          "5.PredUser1Item1" -> ujson.Num(distribpredicted(1, 1, train)), // Datatype of answer: Double
          "6.Mae" -> ujson.Num(0.0) // Datatype of answer: Double
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )            
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}

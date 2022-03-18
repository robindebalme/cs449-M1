package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import scala.collection.mutable
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

  
  val sizeOfTest = test.count()
  val sizeOfTrain = train.count()
  val maxUser = List(train.map(elem => elem.user).max(), test.map(elem => elem.user).max()).max
  val maxItem = List(train.map(elem => elem.item).max(), test.map(elem => elem.item).max()).max


  //val timings = measurements.map(t => t._2) // Retrieve the timing measurements
  
  var allUserAvg = Map(0 -> 0.0)

  val globalAvgDistrib = distribmean(train)

  var alluserAvg_ : mutable.Map[Int, Double] = mutable.Map()
  var allitemAvg_ : mutable.Map[Int, Double] = mutable.Map()
  var allitemDev_ : mutable.Map[Int, Double] = mutable.Map()
  var singleDev_ : mutable.Map[(Double, Double),Double] = mutable.Map()


  def distribmean(df : RDD[Rating]): Double = {
    val pair = df.map(elem => (elem.rating, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }

  def distribUserAvg(user : Int): Double = {
    alluserAvg_.getOrElse(user, {
      val dfFiltered = train.filter(elem => elem.user == user)
      var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg

      else
        tmp = distribmean(dfFiltered)

      alluserAvg_ += ((user, tmp))
      tmp
    })
  }

  def distribItemAvg(item : Int): Double = {
    allitemAvg_.getOrElse(item,{
      val dfFiltered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg
      else
        tmp = distribmean(dfFiltered)

      allitemAvg_ += ((item, tmp))
      tmp
    })
  }


  def distribAvgDev(item : Int): Double = {
    if (allitemDev_.get(item) != None) 
      allitemDev_(item)
    else {
      var tmp = train.filter(l => l.item == item)
      var tmp2 = tmp.map{(elem) =>
        val userAvg = distribUserAvg(elem.user)
        (dev(elem.rating, userAvg) , 1)}.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      val final_dev = tmp2._1 / tmp2._2
      allitemDev_ +=  ((item, final_dev))
      final_dev
    }
  }

  def predicted(user: Int, item : Int): Double = {
    val useravg = distribUserAvg(user)
    if (useravg == globalAvg) {
      globalAvg
    }
    else
      if (distribItemAvg(item) == globalAvg) {
        useravg
      }
      else {
        val avgdev = distribAvgDev(item)
        if (avgdev == 0) {
          useravg
          }
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
      }
    }

  def mae(test: RDD[Rating], train: RDD[Rating], prediction_method: String): Double = {
    if (prediction_method == "GlobalAvg")
      test.map(x => (globalAvg - x.rating).abs).reduce(_ + _)/sizeOfTest
    else if (prediction_method == "UserAvg")
      test.map(x => (distribUserAvg(x.user) - x.rating).abs).reduce(_ + _)/sizeOfTest
    else if (prediction_method == "ItemAvg") 
      test.map(x => (distribItemAvg(x.item) - x.rating).abs).reduce(_ + _)/sizeOfTest
    else if (prediction_method == "DistributedBaselineAvg")
     test.map(x => (predicted(x.user, x.item) - x.rating).abs).reduce(_ + _)/sizeOfTest
    else 0
  }
  
  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val maxUser = List(train.map(elem => elem.user).max(), test.map(elem => elem.user).max()).max
    val maxItem = List(train.map(elem => elem.item).max(), test.map(elem => elem.item).max()).max

    val globalAvg = distribmean(train)

    val alluserAvg = mutable.Map(0 -> 0.0)
    val allitemAvg = mutable.Map(0 -> 0.0)
    val allitemDev = mutable.Map(0 -> 0.0)
    val singleDev : mutable.Map[(Double, Double),Double] = mutable.Map()

    // Thread.sleep(1000) // Do everything here from train and test
    mae(test, train, "UserAvg")  // Output answer as last value
  }))

  val timings = measurements.map(t => t._2)

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
          "2.User1Avg" -> ujson.Num(0.0),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(0.0),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(0.0), // Datatype of answer: Double,
          "5.PredUser1Item1" -> ujson.Num(0.0), // Datatype of answer: Double
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


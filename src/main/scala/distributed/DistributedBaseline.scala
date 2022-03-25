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


  val globalAvgDistrib = distribmeanr(train)

  val userArr = (train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap
  val itemArr = train.map(x => (x.item, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
  val storeDev = itemDevAll(train, userArr).collectAsMap

  def distribmeanr(df : RDD[Rating]): Double = {
    val pair = df.map(elem => (elem.rating, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }

   def distribmean(df : RDD[Double]): Double = {
    val pair = df.map(x => (x, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }

  def itemDevAll(train: RDD[Rating], userArr: collection.Map[Int, Double]): RDD[(Int, Double)] =  {
    train.map(x => (x.item, (dev(x.rating, userArr.getOrElse(x.user, x.rating)), 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1 / x._2))
  }

  def itemDevAllAlone(train: RDD[Rating]): RDD[(Int, Double)] =  {
    val userArr = (train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap
    train.map(x => (x.item, (dev(x.rating, userArr.getOrElse(x.user, x.rating)), 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1 / x._2))
  }

  def predictedDistribBaseline(train: RDD[Rating]): (Int, Int) => Double = {
    val globalAvgDistrib = spark.sparkContext.broadcast(distribmeanr(train))
    val userArr = spark.sparkContext.broadcast((train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap)
    val deviation = spark.sparkContext.broadcast((train.map(x => (x.item, (dev(x.rating, userArr.value.getOrElse(x.user, x.rating)), 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1 / x._2))).collectAsMap)
    (user: Int, item: Int) => {
    val useravg = userArr.value.getOrElse(user, globalAvgDistrib.value)
    if (useravg == globalAvgDistrib.value) globalAvgDistrib.value
    else {
        val avgdev = deviation.value.getOrElse(item, 0.0)
        if (avgdev == 0.0) useravg
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
      }
    }
  }

  def predictedDistribBaselineTest(train: RDD[Rating]): (Int, Int) => Double = {
    val globalAvgDistrib = distribmeanr(train)
    val userArr = ((train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap)
    val deviation = ((train.map(x => (x.item, (dev(x.rating, userArr.getOrElse(x.user, x.rating)), 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1 / x._2))).collectAsMap)
    (user: Int, item: Int) => {
    val useravg = userArr.getOrElse(user, globalAvgDistrib)
    if (useravg == globalAvgDistrib) globalAvgDistrib
    else {
        val avgdev = deviation.getOrElse(item, 0.0)
        if (avgdev == 0.0) useravg
        else
          useravg + avgdev * scale((useravg + avgdev), useravg)
      }
    }
  }

  def predictedDistribGlobal(train: RDD[Rating]): (Int, Int) => Double = {
    val globalAvgDistrib = (distribmeanr(train))
    (user, item) => globalAvgDistrib
  }

  def predictedDistribUser(train: RDD[Rating]): (Int, Int) => Double = {
    val globalAvgDistrib = (distribmeanr(train))
    val userArr = ((train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap)
    (user, item) => userArr.getOrElse(user, globalAvgDistrib)
  }

  def predictedDistribItem(train: RDD[Rating]): (Int, Int) => Double = {
    val globalAvgDistrib = (distribmeanr(train))
    val itemArr = ((train.map(x => (x.item, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)).collectAsMap)
    (user, item) => itemArr.getOrElse(item, globalAvgDistrib)
  }

  def maeDistribTest(test: RDD[Rating], train: RDD[Rating], prediction_method: RDD[Rating] => ((Int, Int) => Double)): Double = {
    val predictor = (prediction_method(train))
    val res = test.map(elem => ((predictor(elem.user, elem.item) - elem.rating).abs, 1)).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    res._1 / res._2.toDouble
  }


  def maeDistrib(test: RDD[Rating], train: RDD[Rating], prediction_method: RDD[Rating] => ((Int, Int) => Double)): Double = {
    val predictor = spark.sparkContext.broadcast(prediction_method(train))
    val res = test.map(elem => ((predictor.value(elem.user, elem.item) - elem.rating).abs, 1)).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    res._1 / res._2.toDouble
  }

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    
    maeDistrib(test, train, predictedDistribBaseline)
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
          "1.GlobalAvg" -> ujson.Num(globalAvgDistrib), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(userArr.getOrElse(1, globalAvgDistrib)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num((itemArr.filter(_._1 == 1)).first._2),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(storeDev.getOrElse(1, 0.0)), // Datatype of answer: Double,
          "5.PredUser1Item1" -> ujson.Num(predictedDistribBaseline(train)(1, 1)), // Datatype of answer: Double
          "6.Mae" -> ujson.Num(maeDistrib(test, train, predictedDistribBaseline)) // Datatype of answer: Double
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


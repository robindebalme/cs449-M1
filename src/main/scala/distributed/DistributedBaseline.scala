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

  
  /*val sizeOfTest = test.count()
  val sizeOfTrain = train.count()
  val maxUser = List(train.map(elem => elem.user).max(), test.map(elem => elem.user).max()).max
  val maxItem = List(train.map(elem => elem.item).max(), test.map(elem => elem.item).max()).max*/


  def distribmean(df : RDD[Rating]): Double = {
    val pair = df.map(elem => (elem.rating, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }

  val globalAvgDistrib = distribmean(train)

  val userArr = train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
  //val itemArr = train.map(x => (x.item, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
  /*
  //var alluserAvg_ : Map[Int, Double] = Map()
  //var allitemAvg_ : Map[Int, Double] = Map()
  //var allitemDev_ : Map[Int, Double] = Map()

  val globalAvgDistrib = distribmean(train)

  val userArr = (data: RDD[Rating]) average_key_value(data.map(x => (x.user, (x.rating, 1)))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
  val itemArr = (data: RDD[Rating]) average_key_value(data.map(x => (x.item, (x.rating, 1)))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)

  val items = test.map(_.item).collect().toSet
  
   

  
  def apu_dev (data: RDD[Rating]) = {
        val average_per_user = userArr(data)
        val deviations = ratings.map(x => (x.user, x))
                            .join(average_per_user)
                            .mapValues(x => (x._1.item, dev(x._1.rating, x._2)))
                            .cache()
        (average_per_user, deviations)
    }
  val apu_Dev = apu_dev(train)

  def distribmean(df : RDD[Rating]): Double = {
    val pair = df.map(elem => (elem.rating, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }

  def distribUseravg(user : Int): Double = {
    val dfFiltered = train.filter(elem => elem.user == user)
    var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg
      else
        tmp = distribmean(dfFiltered)
      tmp
  }

  val alluserAvg_ = spark.sparkContext.broadcast((1 to maxUser).map(u => (u, distribUseravg(u))).toMap)
  val allitemAvg_ = spark.sparkContext.broadcast((1 to maxItem).map(i => (i, distribItemavg(i))).toMap)
  val allitemDev_ = spark.sparkContext.broadcast((1 to maxItem).map(i => (i, distribavgDev(i, alluserAvg_.value))).toMap)

  /*def distribUserAvg(user : Int): Double = {
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
  }*/

  def distribItemavg(item : Int): Double = {
      val dfFiltered = train.filter(elem => elem.item == item)
      var tmp = 0.0
      if (dfFiltered.isEmpty)
        tmp = globalAvg
      else
        tmp = distribmean(dfFiltered)
      tmp
  }

  /*def distribItemAvg(item : Int): Double = {
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
  }*/


  /*def distribAvgDev(item : Int): Double = {
    if (allitemDev_.get(item) != None) 
      allitemDev_(item)
    else {
      var tmp = train.filter(l => l.item == item)
      var tmp2 = tmp.map{(elem) =>
        val userAvg = alluserAvg_.value.getOrElse(elem.user, globalAvgDistrib)
        (dev(elem.rating, userAvg) , 1)}.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      val final_dev = tmp2._1 / tmp2._2
      allitemDev_ +=  ((item, final_dev))
      final_dev
    }
  }*/

  def distribavgDev(item : Int, mapUseravg: Map[Int,Double]): Double = {
      val tmp = train.filter(l => l.item == item)
      if (tmp.isEmpty()) 0.0
      else {var tmp2 = tmp.map{(elem) =>
        val userAvg = mapUseravg.getOrElse(elem.user, globalAvgDistrib)
        (dev(elem.rating, userAvg) , 1)}.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
      val final_dev = tmp2._1 / tmp2._2
      final_dev
      }
  }

  def predictedDistrib(user: Int, item : Int, mapUseravg: Map[Int,Double], mapItemavg: Map[Int,Double], mapDevavg: Map[Int,Double]): Double = {
    val useravg = mapUseravg.getOrElse(user, globalAvgDistrib)
    if (useravg == globalAvg) globalAvg
    else if (mapItemavg.getOrElse(item, useravg) == useravg) useravg
    else {
      val avgdev = (mapDevavg.getOrElse(item, 0.0))
      if (avgdev == 0.0) useravg
      else
        useravg + avgdev * scale((useravg + avgdev), useravg)
    }
  }

  def predictorDistrib(train : RDD[Rating], mapUseravg: Map[Int,Double], mapItemavg: Map[Int,Double], mapDevavg: Map[Int,Double]): (Int, Int) => Double = {
    (user, item) => predictedDistrib(user, item, mapUseravg, mapItemavg, mapDevavg)
  }

  

  def mae(test: RDD[Rating], train: RDD[Rating], prediction_method: (RDD[Rating], Map[Int,Double], Map[Int,Double], Map[Int,Double])  => ((Int, Int) => Double)): Double = {
    val pair = test.map(x => ((prediction_method(train, alluserAvg_.value, allitemAvg_.value, alluserAvg_.value)(x.user, x.item) - x.rating).abs, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    pair._1 / pair._2
  }*/
  
  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    //val maxUser = List(train.map(elem => elem.user).max(), test.map(elem => elem.user).max()).max
    //val maxItem = List(train.map(elem => elem.item).max(), test.map(elem => elem.item).max()).max


    // Thread.sleep(1000) // Do everything here from train and test
    distribmean(train) // Output answer as last value
    //train.map(x => (x.user, (x.rating, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
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
          "2.User1Avg" -> ujson.Num((userArr.filter(_._1 == 1)).first._2),  // Datatype of answer: Double
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


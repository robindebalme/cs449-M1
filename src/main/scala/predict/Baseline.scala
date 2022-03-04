package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}


object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

  val globalAvg = train.foldLeft(0.0){(acc : Double, arr: Rating) => acc + arr.rating} / train.length

  val user1Avg = train.filter(arr => arr.user == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == 1).length

  val item1Avg = train.filter(arr => arr.item == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.item == 1).length

  def anyUserAvg(user : Int, arr : Array[Rating]) : Double = {
    val arrFiltered = arr.filter(arr => arr.user == user)
    if (arrFiltered.isEmpty)
      globalAvg
    else
     arrFiltered.foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == user).length
  }

  def anyItemAvg(item : Int, arr : Array[Rating]) : Double =  {
    val arrFiltered = arr.filter(arr => arr.item == item)
    if (arrFiltered.isEmpty)
      globalAvg
    else
      arrFiltered.foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.item == item).length
    }
    
  def scale(x : Double, userAvg : Double): Double = {
    if (x > userAvg)
      5 - userAvg
    else if (x < userAvg)
      userAvg - 1
    else 1
  }

  def anyAvgDev(item : Int, arr : Array[Rating]): Double = {
    arr.filter(l => l.item == item).foldLeft(0.0){(acc, elem) => 
      val userAvg = anyUserAvg(elem.user, arr)
      (elem.rating - userAvg) / scale(elem.rating, userAvg) + acc} / arr.filter(l => l.item == item).length
  }

  def predicted(user: Int, item : Int, arr : Array[Rating]): Double = {
    val userAvg = anyUserAvg(user, arr)
    if (arr.filter(l => l.item == item).isEmpty)
      userAvg
    else {
      val avgDev = anyAvgDev(item, arr)
      userAvg + avgDev * scale((userAvg + avgDev), userAvg)
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
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(globalAvg), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(anyUserAvg(1, train)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(anyItemAvg(1, train)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(anyAvgDev(1, train)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(predicted(1, 1, train)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(0.0), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(0.0),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(0.0),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(0.0)   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}

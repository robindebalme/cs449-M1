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

  val GlobalAvg = train.foldLeft(0.0){(acc : Double, arr: Rating) => acc + arr.rating} / train.length

  val User1Avg = train.filter(arr => arr.user == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == 1).length

  val Item1Avg = train.filter(arr => arr.item == 1).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.item == 1).length

  def AnyUserAvg(user : Int, arr : Array[Rating]) : Double =  train.filter(arr => arr.user == user).foldLeft(0.0){(acc, arr) => acc + arr.rating} / train.filter(arr => arr.user == user).length
  
  def scale(x : Int, UserAvg : Double): Double = {
    if (x > UserAvg)
      5 - UserAvg
    else if (x < UserAvg)
      UserAvg - 1
    else 1
  }

  def NormalizedDev(user : Int, item : Int, arr : Array[Rating]) : Double = {
    val r_u_i = arr.filter(x => (x.user == user && x.item == item))
    val UserAvg = AnyUserAvg(user, arr)
    (r_u_i - UserAvg) / scale(r_u_i, UserAvg)
  }
  def AvgDev(user : Int, item : Int, arr : Array[Rating]): Double = {
    arr.filter()
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
          "1.GlobalAvg" -> ujson.Num(GlobalAvg), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(User1Avg),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(Item1Avg),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(0.0), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(0.0) // Datatype of answer: Double
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

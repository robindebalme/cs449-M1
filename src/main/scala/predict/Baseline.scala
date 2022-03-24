package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable
import scala.math._
import scala.math
import shared.predictions._
import scala.collection



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

  globalAvg =  computeGlobalAvg(train)
  val userArr = train.groupBy(_.user).map(x => (x._1, computeGlobalAvg(x._2)))
  val devArr = train.groupBy(_.item).map(x => (x._1, mean_(x._2.map(elem => dev(elem.rating, userArr.getOrElse(elem.user, elem.rating))))))

  val measurements1 = (1 to conf.num_measurements()).map(x => timingInMs(() => {

    mae(test, train, predictorGlobal)

  }))

  val timingsGlobalAvg = measurements1.map(t => t._2)
  
  val measurements2 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    mae(test, train, predictorUser)
    
  }))

  val timingsUserAvg = measurements2.map(t => t._2)

  val measurements3 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    
    mae(test, train, predictorItem)
    
  }))

  val timingsItemAvg = measurements3.map(t => t._2)

  val measurements4 = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    
    mae(test, train, predictorBaseline)
    
  })) 

  val timingsBaselineAvg = measurements4.map(t => t._2)

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
          "2.User1Avg" -> ujson.Num(computeGlobalAvg(train.filter(_.user == 1))),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(computeGlobalAvg(train.filter(_.item == 1))),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(devArr.getOrElse(1, 0.0)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(predictorBaseline(train)(1,1)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(mae(test, train, predictorGlobal)), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(mae(test, train, predictorUser)),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(mae(test, train, predictorItem)),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(mae(test, train, predictorBaseline))   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsGlobalAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsGlobalAvg)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsUserAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsUserAvg)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsItemAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsItemAvg)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsBaselineAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsBaselineAvg)) // Datatype of answer: Double
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
